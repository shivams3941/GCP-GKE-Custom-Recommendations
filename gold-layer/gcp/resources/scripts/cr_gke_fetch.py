"""
Fetch GCP GKE node pool machine type SKUs from the GCP Cloud Billing Catalog API.

GKE pricing is based on:
1. VM machine types used for node pools (e2, n2, n1, c2, etc.)
2. Cluster management fee ($0.10/hr for Standard clusters, free for Autopilot per-pod billing)

This script fetches machine type SKUs for a given GCP region and writes
a structured JSON catalog to gold-layer/gcp/resources/skus/cr_gke_sku.json.

Usage:
    python cr_gke_fetch.py <region> [output_file]

Example:
    python cr_gke_fetch.py us-central1
    python cr_gke_fetch.py us-central1 cr_gke_sku.json
"""

import json
import os
import sys
import requests
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# GCP Cloud Billing SKU API
# ---------------------------------------------------------------------------
# GCE service ID (Compute Engine — covers all GKE node VM pricing)
GCE_SERVICE_ID = "6F81-5844-456A"
SKU_API_URL = f"https://cloudbilling.googleapis.com/v1/services/{GCE_SERVICE_ID}/skus"

# Path to service account key — checks env var first, then uses key in gold-layer/gcp/
_DEFAULT_KEY_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..",
    "devops-internal-439011-f4e928045fd3.json"
)
SERVICE_ACCOUNT_KEY = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", os.path.abspath(_DEFAULT_KEY_PATH))

# Regions we care about — maps GCP region prefix to billing region description substring
REGION_DESCRIPTION_MAP = {
    "us-central1":    "Iowa",
    "us-east1":       "South Carolina",
    "us-east4":       "Northern Virginia",
    "us-west1":       "Oregon",
    "us-west2":       "Los Angeles",
    "us-west3":       "Salt Lake City",
    "us-west4":       "Las Vegas",
    "europe-west1":   "Belgium",
    "europe-west2":   "London",
    "europe-west3":   "Frankfurt",
    "europe-west4":   "Netherlands",
    "asia-east1":     "Taiwan",
    "asia-northeast1": "Tokyo",
    "asia-southeast1": "Singapore",
    "australia-southeast1": "Sydney",
}

# Machine families supported by GKE (Standard clusters)
GKE_MACHINE_FAMILIES = ["e2", "n1", "n2", "n2d", "c2", "c2d", "c3", "c3d", "n4", "t2d", "t2a", "m1", "m2", "a2"]

HOURS_PER_MONTH = 730
HOURS_PER_YEAR = 8760


def _get_auth_token() -> str:
    """Get a Bearer token from the service account key file."""
    try:
        import google.auth.transport.requests
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        creds.refresh(google.auth.transport.requests.Request())
        return creds.token
    except Exception as e:
        raise RuntimeError(
            f"Failed to get auth token from service account key.\n"
            f"Key path: {SERVICE_ACCOUNT_KEY}\n"
            f"Error: {e}\n\n"
            f"Fix: set GOOGLE_APPLICATION_CREDENTIALS env var to your key file path."
        )


def fetch_all_skus(api_key: Optional[str] = None) -> List[Dict]:
    """Fetch all Compute Engine SKUs from the GCP Billing Catalog API."""
    token = _get_auth_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"pageSize": 5000}

    items = []
    url = SKU_API_URL
    page = 0

    print("Fetching GCP Compute Engine SKUs from Billing Catalog API...")
    while url:
        resp = requests.get(url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("skus", [])
        items.extend(batch)
        page += 1
        print(f"  Page {page}: {len(batch)} SKUs (total: {len(items)})")

        next_token = data.get("nextPageToken")
        if next_token:
            params = {"pageSize": 5000, "pageToken": next_token}
        else:
            url = None

    return items


def _extract_hourly_price(sku: Dict) -> float:
    """Extract the on-demand hourly price (USD) from a SKU pricing info block."""
    try:
        pricing_info = sku.get("pricingInfo", [])
        if not pricing_info:
            return 0.0
        tiered_rates = pricing_info[0].get("pricingExpression", {}).get("tieredRates", [])
        for rate in tiered_rates:
            unit_price = rate.get("unitPrice", {})
            nanos = int(unit_price.get("nanos", 0))
            units = int(unit_price.get("units", 0))
            price = units + nanos / 1e9
            if price > 0:
                return round(price, 8)
    except Exception:
        pass
    return 0.0


def _get_machine_family(description: str) -> Optional[str]:
    """Infer machine family from SKU description.

    Matches family names so that 'c2' does not match 'c2d' descriptions,
    and 'n2' does not match 'n2d' descriptions.
    Uses negative lookahead to ensure the family prefix is not followed
    by another alphanumeric character that would make it a different family.
    """
    import re
    desc_lower = description.lower()
    # Sort by length descending so longer names are tried first (c2d before c2, n2d before n2)
    for family in sorted(GKE_MACHINE_FAMILIES, key=len, reverse=True):
        # Match family only when NOT followed by another letter/digit (e.g. 'c2' not in 'c2d')
        if re.search(re.escape(family) + r'(?![a-z0-9])', desc_lower):
            return family
    return None


def _is_preemptible(description: str) -> bool:
    return "preemptible" in description.lower() or "spot" in description.lower()


def _is_windows(description: str) -> bool:
    return "windows" in description.lower()


def filter_gke_skus(skus: List[Dict], region: str) -> List[Dict]:
    """Filter SKUs to GKE-relevant compute (CPU/RAM) for a given region."""
    region_hint = REGION_DESCRIPTION_MAP.get(region, region)
    filtered = []

    for sku in skus:
        desc = sku.get("description", "")
        category = sku.get("category", {})
        resource_family = category.get("resourceFamily", "")
        resource_group = category.get("resourceGroup", "")
        usage_type = category.get("usageType", "")

        # Only Compute family, CPU or RAM resource groups
        if resource_family != "Compute":
            continue
        if resource_group not in ("CPU", "RAM", "N1Standard", "N2Standard", "E2", "C2", "C2D", "N2D", "T2D", "T2A", "N1Highmem", "N1Highcpu"):
            continue

        # Only OnDemand (skip committed use, sustained use adjustments)
        if usage_type not in ("OnDemand",):
            continue

        # Skip preemptible and Windows
        if _is_preemptible(desc) or _is_windows(desc):
            continue

        # Region filter — check service regions list
        service_regions = sku.get("serviceRegions", [])
        if region not in service_regions:
            continue

        filtered.append(sku)

    return filtered


def build_machine_type_catalog(skus: List[Dict], region: str) -> Dict[str, Dict]:
    """
    Build a machine-type-level catalog grouped by machine family.

    GKE node pools use predefined machine types (e.g. e2-standard-4).
    We group by family and store per-vCPU and per-GB-RAM hourly prices
    so the engine can compute cost for any machine type.

    Output structure:
    {
        "<machine_family>": {
            "family": "e2",
            "region": "us-central1",
            "cpu_hourly_price": 0.02289,
            "ram_hourly_price_per_gb": 0.003067,
            "skus": [ <raw sku entries> ]
        }
    }
    """
    catalog: Dict[str, Dict] = {}

    for sku in skus:
        desc = sku.get("description", "")
        family = _get_machine_family(desc)
        if not family:
            continue

        hourly = _extract_hourly_price(sku)
        if hourly == 0.0:
            continue

        resource_group = sku.get("category", {}).get("resourceGroup", "")
        is_cpu = resource_group in ("CPU", "N1Standard", "N2Standard", "E2", "C2", "C2D", "N2D", "T2D", "T2A", "N1Highmem", "N1Highcpu")
        is_ram = resource_group == "RAM"

        # N1 bills CPU and RAM under the same "N1Standard" resource group.
        # Distinguish by checking the description keyword.
        if resource_group == "N1Standard":
            desc_lower = desc.lower()
            if "ram" in desc_lower or "memory" in desc_lower:
                is_cpu = False
                is_ram = True
            else:
                is_cpu = True
                is_ram = False

        if family not in catalog:
            catalog[family] = {
                "family": family,
                "region": region,
                "cpu_hourly_price": 0.0,
                "ram_hourly_price_per_gb": 0.0,
                "skus": [],
            }

        # Use only standard "Instance Core / Instance Ram" SKUs for base pricing.
        # Skip Custom, Extended, Sole Tenancy, and Premium variants — they are
        # add-on rates, not the base on-demand price.
        desc_lower = desc.lower()
        is_standard_sku = (
            "custom" not in desc_lower
            and "extended" not in desc_lower
            and "sole tenancy" not in desc_lower
            and "premium" not in desc_lower
        )
        if is_cpu and is_standard_sku:
            if hourly > catalog[family]["cpu_hourly_price"]:
                catalog[family]["cpu_hourly_price"] = hourly
        if is_ram and is_standard_sku:
            if hourly > catalog[family]["ram_hourly_price_per_gb"]:
                catalog[family]["ram_hourly_price_per_gb"] = hourly

        catalog[family]["skus"].append({
            "sku_id": sku.get("skuId", ""),
            "description": desc,
            "resource_group": resource_group,
            "hourly_price": hourly,
        })

    # c2 (Intel Cascade Lake compute-optimized) is not returned by the Billing API
    # under a distinct resource group for all regions. Inject hardcoded official prices.
    # Source: https://cloud.google.com/compute/vm-instance-pricing (us-central1)
    if "c2" not in catalog:
        catalog["c2"] = {
            "family": "c2",
            "region": region,
            "cpu_hourly_price": 0.03398,
            "ram_hourly_price_per_gb": 0.00455,
            "skus": [
                {"sku_id": "hardcoded", "description": "C2 Instance Core running in Americas (hardcoded)", "resource_group": "CPU", "hourly_price": 0.03398},
                {"sku_id": "hardcoded", "description": "C2 Instance Ram running in Americas (hardcoded)", "resource_group": "RAM", "hourly_price": 0.00455},
            ],
        }

    return catalog


def enrich_with_machine_types(catalog: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Add a predefined list of common GKE machine types per family with
    computed hourly/monthly/annual costs and ordinals (cheaper = lower ordinal).

    Machine type specs source: https://cloud.google.com/compute/docs/machine-resource
    """
    # (machine_type, vcpus, ram_gb)
    MACHINE_TYPES: Dict[str, List] = {
        "e2": [
            ("e2-micro",       0.25, 1),
            ("e2-small",       0.5,  2),
            ("e2-medium",      1,    4),
            ("e2-standard-2",  2,    8),
            ("e2-standard-4",  4,    16),
            ("e2-standard-8",  8,    32),
            ("e2-standard-16", 16,   64),
            ("e2-standard-32", 32,   128),
            ("e2-highmem-2",   2,    16),
            ("e2-highmem-4",   4,    32),
            ("e2-highmem-8",   8,    64),
            ("e2-highmem-16",  16,   128),
            ("e2-highcpu-2",   2,    2),
            ("e2-highcpu-4",   4,    4),
            ("e2-highcpu-8",   8,    8),
            ("e2-highcpu-16",  16,   16),
            ("e2-highcpu-32",  32,   32),
        ],
        "n1": [
            ("n1-standard-1",  1,  3.75),
            ("n1-standard-2",  2,  7.5),
            ("n1-standard-4",  4,  15),
            ("n1-standard-8",  8,  30),
            ("n1-standard-16", 16, 60),
            ("n1-standard-32", 32, 120),
            ("n1-standard-64", 64, 240),
            ("n1-standard-96", 96, 360),
            ("n1-highmem-2",   2,  13),
            ("n1-highmem-4",   4,  26),
            ("n1-highmem-8",   8,  52),
            ("n1-highmem-16",  16, 104),
            ("n1-highmem-32",  32, 208),
            ("n1-highmem-64",  64, 416),
            ("n1-highmem-96",  96, 624),
            ("n1-highcpu-2",   2,  1.8),
            ("n1-highcpu-4",   4,  3.6),
            ("n1-highcpu-8",   8,  7.2),
            ("n1-highcpu-16",  16, 14.4),
            ("n1-highcpu-32",  32, 28.8),
            ("n1-highcpu-64",  64, 57.6),
            ("n1-highcpu-96",  96, 86.4),
        ],
        "n2": [
            ("n2-standard-2",  2,  8),
            ("n2-standard-4",  4,  16),
            ("n2-standard-8",  8,  32),
            ("n2-standard-16", 16, 64),
            ("n2-standard-32", 32, 128),
            ("n2-standard-48", 48, 192),
            ("n2-standard-64", 64, 256),
            ("n2-standard-80", 80, 320),
            ("n2-standard-96", 96, 384),
            ("n2-standard-128",128,512),
            ("n2-highmem-2",   2,  16),
            ("n2-highmem-4",   4,  32),
            ("n2-highmem-8",   8,  64),
            ("n2-highmem-16",  16, 128),
            ("n2-highmem-32",  32, 256),
            ("n2-highmem-48",  48, 384),
            ("n2-highmem-64",  64, 512),
            ("n2-highmem-80",  80, 640),
            ("n2-highcpu-2",   2,  2),
            ("n2-highcpu-4",   4,  4),
            ("n2-highcpu-8",   8,  8),
            ("n2-highcpu-16",  16, 16),
            ("n2-highcpu-32",  32, 32),
            ("n2-highcpu-48",  48, 48),
            ("n2-highcpu-64",  64, 64),
            ("n2-highcpu-80",  80, 80),
            ("n2-highcpu-96",  96, 96),
        ],
        "n2d": [
            ("n2d-standard-2",  2,  8),
            ("n2d-standard-4",  4,  16),
            ("n2d-standard-8",  8,  32),
            ("n2d-standard-16", 16, 64),
            ("n2d-standard-32", 32, 128),
            ("n2d-standard-48", 48, 192),
            ("n2d-standard-64", 64, 256),
            ("n2d-standard-96", 96, 384),
            ("n2d-standard-128",128,512),
            ("n2d-standard-224",224,896),
            ("n2d-highmem-2",   2,  16),
            ("n2d-highmem-4",   4,  32),
            ("n2d-highmem-8",   8,  64),
            ("n2d-highmem-16",  16, 128),
            ("n2d-highmem-32",  32, 256),
            ("n2d-highmem-48",  48, 384),
            ("n2d-highmem-64",  64, 512),
            ("n2d-highmem-96",  96, 768),
            ("n2d-highcpu-2",   2,  2),
            ("n2d-highcpu-4",   4,  4),
            ("n2d-highcpu-8",   8,  8),
            ("n2d-highcpu-16",  16, 16),
            ("n2d-highcpu-32",  32, 32),
            ("n2d-highcpu-48",  48, 48),
            ("n2d-highcpu-64",  64, 64),
            ("n2d-highcpu-96",  96, 96),
        ],
        "c2": [
            ("c2-standard-4",  4,  16),
            ("c2-standard-8",  8,  32),
            ("c2-standard-16", 16, 64),
            ("c2-standard-30", 30, 120),
            ("c2-standard-60", 60, 240),
        ],
        "c2d": [
            ("c2d-standard-2",  2,  8),
            ("c2d-standard-4",  4,  16),
            ("c2d-standard-8",  8,  32),
            ("c2d-standard-16", 16, 64),
            ("c2d-standard-32", 32, 128),
            ("c2d-standard-56", 56, 224),
            ("c2d-standard-112",112,448),
            ("c2d-highmem-2",   2,  16),
            ("c2d-highmem-4",   4,  32),
            ("c2d-highmem-8",   8,  64),
            ("c2d-highmem-16",  16, 128),
            ("c2d-highmem-32",  32, 256),
            ("c2d-highmem-56",  56, 448),
            ("c2d-highmem-112", 112,896),
            ("c2d-highcpu-2",   2,  2),
            ("c2d-highcpu-4",   4,  4),
            ("c2d-highcpu-8",   8,  8),
            ("c2d-highcpu-16",  16, 16),
            ("c2d-highcpu-32",  32, 32),
            ("c2d-highcpu-56",  56, 56),
            ("c2d-highcpu-112", 112,112),
        ],
        "t2d": [
            ("t2d-standard-1",  1,  4),
            ("t2d-standard-2",  2,  8),
            ("t2d-standard-4",  4,  16),
            ("t2d-standard-8",  8,  32),
            ("t2d-standard-16", 16, 64),
            ("t2d-standard-32", 32, 128),
            ("t2d-standard-48", 48, 192),
            ("t2d-standard-60", 60, 240),
        ],
        # --- 2nd gen AMD ---
        "n2d": [
            ("n2d-standard-2",   2,   8),
            ("n2d-standard-4",   4,   16),
            ("n2d-standard-8",   8,   32),
            ("n2d-standard-16",  16,  64),
            ("n2d-standard-32",  32,  128),
            ("n2d-standard-48",  48,  192),
            ("n2d-standard-64",  64,  256),
            ("n2d-standard-96",  96,  384),
            ("n2d-standard-128", 128, 512),
            ("n2d-standard-224", 224, 896),
            ("n2d-highmem-2",    2,   16),
            ("n2d-highmem-4",    4,   32),
            ("n2d-highmem-8",    8,   64),
            ("n2d-highmem-16",   16,  128),
            ("n2d-highmem-32",   32,  256),
            ("n2d-highmem-48",   48,  384),
            ("n2d-highmem-64",   64,  512),
            ("n2d-highmem-96",   96,  768),
            ("n2d-highcpu-2",    2,   2),
            ("n2d-highcpu-4",    4,   4),
            ("n2d-highcpu-8",    8,   8),
            ("n2d-highcpu-16",   16,  16),
            ("n2d-highcpu-32",   32,  32),
            ("n2d-highcpu-48",   48,  48),
            ("n2d-highcpu-64",   64,  64),
            ("n2d-highcpu-96",   96,  96),
        ],
        "c2d": [
            ("c2d-standard-2",   2,   8),
            ("c2d-standard-4",   4,   16),
            ("c2d-standard-8",   8,   32),
            ("c2d-standard-16",  16,  64),
            ("c2d-standard-32",  32,  128),
            ("c2d-standard-56",  56,  224),
            ("c2d-standard-112", 112, 448),
            ("c2d-highmem-2",    2,   16),
            ("c2d-highmem-4",    4,   32),
            ("c2d-highmem-8",    8,   64),
            ("c2d-highmem-16",   16,  128),
            ("c2d-highmem-32",   32,  256),
            ("c2d-highmem-56",   56,  448),
            ("c2d-highmem-112",  112, 896),
            ("c2d-highcpu-2",    2,   2),
            ("c2d-highcpu-4",    4,   4),
            ("c2d-highcpu-8",    8,   8),
            ("c2d-highcpu-16",   16,  16),
            ("c2d-highcpu-32",   32,  32),
            ("c2d-highcpu-56",   56,  56),
            ("c2d-highcpu-112",  112, 112),
        ],
        # --- Arm ---
        "t2a": [
            ("t2a-standard-1",  1,  4),
            ("t2a-standard-2",  2,  8),
            ("t2a-standard-4",  4,  16),
            ("t2a-standard-8",  8,  32),
            ("t2a-standard-16", 16, 64),
            ("t2a-standard-32", 32, 128),
            ("t2a-standard-48", 48, 192),
        ],
    }

    for family, machine_list in MACHINE_TYPES.items():
        if family not in catalog:
            continue

        cpu_price = catalog[family]["cpu_hourly_price"]
        ram_price = catalog[family]["ram_hourly_price_per_gb"]

        machine_entries = []
        for machine_type, vcpus, ram_gb in machine_list:
            hourly = round(vcpus * cpu_price + ram_gb * ram_price, 6)
            machine_entries.append({
                "machine_type": machine_type,
                "family": family,
                "vcpus": vcpus,
                "ram_gb": ram_gb,
                "hourly_price": hourly,
                "monthly_price": round(hourly * HOURS_PER_MONTH, 4),
                "annual_price": round(hourly * HOURS_PER_YEAR, 4),
            })

        # Assign ordinals by price (cheapest = 1)
        unique_prices = sorted(set(e["hourly_price"] for e in machine_entries))
        price_to_ordinal = {p: i + 1 for i, p in enumerate(unique_prices)}
        for entry in machine_entries:
            entry["ordinal"] = price_to_ordinal[entry["hourly_price"]]

        machine_entries.sort(key=lambda x: x["ordinal"])
        catalog[family]["machine_types"] = machine_entries

    return catalog


def main():
    if len(sys.argv) < 2:
        print("Usage: python cr_gke_fetch.py <region> [output_file]")
        print("\nExample: python cr_gke_fetch.py us-central1")
        sys.exit(1)

    region = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        os.path.dirname(__file__), "..", "skus", "cr_gke_sku.json"
    )

    api_key = os.environ.get("GCP_API_KEY")  # no longer needed — kept for compatibility

    print(f"\n=== Building GKE SKU catalog for region: {region} ===\n")

    all_skus = fetch_all_skus()
    print(f"\nTotal SKUs fetched: {len(all_skus)}")

    filtered = filter_gke_skus(all_skus, region)
    print(f"GKE-relevant SKUs after filtering: {len(filtered)}")

    catalog = build_machine_type_catalog(filtered, region)
    catalog = enrich_with_machine_types(catalog)

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(catalog, f, indent=2)

    total_machine_types = sum(len(v.get("machine_types", [])) for v in catalog.values())
    print(f"\n=== Summary ===")
    print(f"Machine families: {len(catalog)}")
    print(f"Total machine types: {total_machine_types}")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    main()
