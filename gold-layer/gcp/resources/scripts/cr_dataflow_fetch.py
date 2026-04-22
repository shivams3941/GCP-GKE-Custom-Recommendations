"""
Fetch GCP Dataflow worker machine type SKUs from the GCP Cloud Billing Catalog API.

Dataflow worker pricing is based on Compute Engine machine types used for workers.
The default worker type is n1-standard-4 when not explicitly configured.

This script fetches machine type SKUs for a given GCP region and writes
a structured JSON catalog to gold-layer/gcp/resources/skus/cr_dataflow_sku.json.

Usage:
    python cr_dataflow_fetch.py <region> [output_file]

Example:
    python cr_dataflow_fetch.py us-central1
    python cr_dataflow_fetch.py us-central1 cr_dataflow_sku.json
"""

import json
import os
import sys
import requests
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# GCP Cloud Billing SKU API
# ---------------------------------------------------------------------------
GCE_SERVICE_ID = "6F81-5844-456A"
SKU_API_URL = f"https://cloudbilling.googleapis.com/v1/services/{GCE_SERVICE_ID}/skus"

_DEFAULT_KEY_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..",
    "devops-internal-439011-f4e928045fd3.json"
)
SERVICE_ACCOUNT_KEY = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", os.path.abspath(_DEFAULT_KEY_PATH))

REGION_DESCRIPTION_MAP = {
    "us-central1":    "Iowa",
    "us-east1":       "South Carolina",
    "us-east4":       "Northern Virginia",
    "us-west1":       "Oregon",
    "us-west2":       "Los Angeles",
    "europe-west1":   "Belgium",
    "europe-west2":   "London",
    "europe-west3":   "Frankfurt",
    "europe-west4":   "Netherlands",
    "asia-east1":     "Taiwan",
    "asia-northeast1": "Tokyo",
    "asia-southeast1": "Singapore",
}

# Dataflow supports these machine families for workers
DATAFLOW_MACHINE_FAMILIES = ["n1", "n2", "n2d", "e2", "c2", "c2d", "t2d"]

HOURS_PER_MONTH = 730
HOURS_PER_YEAR = 8760


def _get_auth_token() -> str:
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
            f"Error: {e}"
        )


def fetch_all_skus() -> List[Dict]:
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
    desc_lower = description.lower()
    for family in DATAFLOW_MACHINE_FAMILIES:
        if family in desc_lower:
            return family
    return None


def _is_preemptible(description: str) -> bool:
    return "preemptible" in description.lower() or "spot" in description.lower()


def filter_dataflow_skus(skus: List[Dict], region: str) -> List[Dict]:
    """Filter SKUs to Dataflow-relevant compute (CPU/RAM) for a given region."""
    filtered = []

    for sku in skus:
        desc = sku.get("description", "")
        category = sku.get("category", {})
        resource_family = category.get("resourceFamily", "")
        resource_group = category.get("resourceGroup", "")
        usage_type = category.get("usageType", "")

        if resource_family != "Compute":
            continue
        if resource_group not in ("CPU", "RAM", "N1Standard", "N2Standard", "E2", "C2", "C2D", "N2D", "T2D",
                                   "N1Highmem", "N1Highcpu"):
            continue
        if usage_type not in ("OnDemand",):
            continue
        if _is_preemptible(desc):
            continue

        service_regions = sku.get("serviceRegions", [])
        if region not in service_regions:
            continue

        filtered.append(sku)

    return filtered


def build_machine_type_catalog(skus: List[Dict], region: str) -> Dict[str, Dict]:
    """Build a machine-type-level catalog grouped by machine family."""
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
        is_cpu = resource_group in ("CPU", "N1Standard", "N2Standard", "E2", "C2", "C2D", "N2D", "T2D",
                                     "N1Highmem", "N1Highcpu")
        is_ram = resource_group == "RAM"

        if family not in catalog:
            catalog[family] = {
                "family": family,
                "region": region,
                "cpu_hourly_price": 0.0,
                "ram_hourly_price_per_gb": 0.0,
                "skus": [],
            }

        if is_cpu and catalog[family]["cpu_hourly_price"] == 0.0:
            catalog[family]["cpu_hourly_price"] = hourly
        if is_ram and catalog[family]["ram_hourly_price_per_gb"] == 0.0:
            catalog[family]["ram_hourly_price_per_gb"] = hourly

        catalog[family]["skus"].append({
            "sku_id": sku.get("skuId", ""),
            "description": desc,
            "resource_group": resource_group,
            "hourly_price": hourly,
        })

    return catalog


def enrich_with_machine_types(catalog: Dict[str, Dict]) -> Dict[str, Dict]:
    """Add predefined Dataflow worker machine types per family with computed costs."""
    # Common Dataflow worker machine types (machine_type, vcpus, ram_gb)
    MACHINE_TYPES: Dict[str, List] = {
        "n1": [
            ("n1-standard-1",  1,  3.75),
            ("n1-standard-2",  2,  7.5),
            ("n1-standard-4",  4,  15),
            ("n1-standard-8",  8,  30),
            ("n1-standard-16", 16, 60),
            ("n1-standard-32", 32, 120),
            ("n1-standard-64", 64, 240),
            ("n1-highmem-2",   2,  13),
            ("n1-highmem-4",   4,  26),
            ("n1-highmem-8",   8,  52),
            ("n1-highmem-16",  16, 104),
            ("n1-highmem-32",  32, 208),
            ("n1-highcpu-2",   2,  1.8),
            ("n1-highcpu-4",   4,  3.6),
            ("n1-highcpu-8",   8,  7.2),
            ("n1-highcpu-16",  16, 14.4),
            ("n1-highcpu-32",  32, 28.8),
        ],
        "n2": [
            ("n2-standard-2",  2,  8),
            ("n2-standard-4",  4,  16),
            ("n2-standard-8",  8,  32),
            ("n2-standard-16", 16, 64),
            ("n2-standard-32", 32, 128),
            ("n2-standard-48", 48, 192),
            ("n2-standard-64", 64, 256),
            ("n2-highmem-2",   2,  16),
            ("n2-highmem-4",   4,  32),
            ("n2-highmem-8",   8,  64),
            ("n2-highmem-16",  16, 128),
            ("n2-highcpu-2",   2,  2),
            ("n2-highcpu-4",   4,  4),
            ("n2-highcpu-8",   8,  8),
            ("n2-highcpu-16",  16, 16),
            ("n2-highcpu-32",  32, 32),
        ],
        "e2": [
            ("e2-standard-2",  2,  8),
            ("e2-standard-4",  4,  16),
            ("e2-standard-8",  8,  32),
            ("e2-standard-16", 16, 64),
            ("e2-standard-32", 32, 128),
            ("e2-highmem-2",   2,  16),
            ("e2-highmem-4",   4,  32),
            ("e2-highmem-8",   8,  64),
            ("e2-highmem-16",  16, 128),
            ("e2-highcpu-2",   2,  2),
            ("e2-highcpu-4",   4,  4),
            ("e2-highcpu-8",   8,  8),
            ("e2-highcpu-16",  16, 16),
            ("e2-highcpu-32",  32, 32),
        ],
        "c2": [
            ("c2-standard-4",  4,  16),
            ("c2-standard-8",  8,  32),
            ("c2-standard-16", 16, 64),
            ("c2-standard-30", 30, 120),
            ("c2-standard-60", 60, 240),
        ],
        "n2d": [
            ("n2d-standard-2",  2,  8),
            ("n2d-standard-4",  4,  16),
            ("n2d-standard-8",  8,  32),
            ("n2d-standard-16", 16, 64),
            ("n2d-standard-32", 32, 128),
            ("n2d-standard-48", 48, 192),
            ("n2d-standard-64", 64, 256),
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

        unique_prices = sorted(set(e["hourly_price"] for e in machine_entries))
        price_to_ordinal = {p: i + 1 for i, p in enumerate(unique_prices)}
        for entry in machine_entries:
            entry["ordinal"] = price_to_ordinal[entry["hourly_price"]]

        machine_entries.sort(key=lambda x: x["ordinal"])
        catalog[family]["machine_types"] = machine_entries

    return catalog


def main():
    if len(sys.argv) < 2:
        print("Usage: python cr_dataflow_fetch.py <region> [output_file]")
        print("\nExample: python cr_dataflow_fetch.py us-central1")
        sys.exit(1)

    region = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        os.path.dirname(__file__), "..", "skus", "cr_dataflow_sku.json"
    )

    print(f"\n=== Building Dataflow SKU catalog for region: {region} ===\n")

    all_skus = fetch_all_skus()
    print(f"\nTotal SKUs fetched: {len(all_skus)}")

    filtered = filter_dataflow_skus(all_skus, region)
    print(f"Dataflow-relevant SKUs after filtering: {len(filtered)}")

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
