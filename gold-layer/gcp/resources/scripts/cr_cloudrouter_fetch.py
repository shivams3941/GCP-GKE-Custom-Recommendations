"""
Fetch GCP Cloud Router / Cloud NAT SKU pricing from the GCP Cloud Billing Catalog API.

Cloud Router itself has no per-hour charge. Cost is driven by:
1. VPN tunnels attached to the router ($0.05/hr per tunnel)
2. Cloud NAT: per NAT IP address ($0.004/hr) + per GB processed

This script fetches VPN tunnel and NAT IP SKUs for a given GCP region and writes
a structured JSON catalog to gold-layer/gcp/resources/skus/cr_cloudrouter_sku.json.

Usage:
    python cr_cloudrouter_fetch.py <region> [output_file]

Example:
    python cr_cloudrouter_fetch.py us-central1
    python cr_cloudrouter_fetch.py us-central1 cr_cloudrouter_sku.json
"""

import json
import os
import sys
import requests
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# GCP Cloud Billing SKU API
# ---------------------------------------------------------------------------
# Compute Engine service ID (covers VPN tunnels and Cloud NAT)
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


def filter_cloudrouter_skus(skus: List[Dict], region: str) -> Dict[str, List[Dict]]:
    """Filter SKUs to Cloud Router / NAT relevant entries for a given region."""
    vpn_skus = []
    nat_skus = []

    for sku in skus:
        desc = sku.get("description", "").lower()
        service_regions = sku.get("serviceRegions", [])

        if region not in service_regions:
            continue

        if "vpn tunnel" in desc and "preemptible" not in desc:
            vpn_skus.append(sku)
        elif "cloud nat" in desc or ("nat" in desc and "ip" in desc):
            nat_skus.append(sku)

    return {"vpn_tunnel": vpn_skus, "nat_ip": nat_skus}


def build_cloudrouter_catalog(filtered: Dict[str, List[Dict]], region: str) -> Dict:
    """Build a pricing catalog for Cloud Router cost components."""
    catalog = {}

    # VPN tunnel pricing
    vpn_hourly = 0.05  # default fallback
    for sku in filtered.get("vpn_tunnel", []):
        price = _extract_hourly_price(sku)
        if price > 0:
            vpn_hourly = price
            break

    catalog["vpn_tunnel"] = {
        "region": region,
        "description": "VPN tunnel attached to Cloud Router",
        "hourly_price": vpn_hourly,
        "monthly_price": round(vpn_hourly * HOURS_PER_MONTH, 4),
        "annual_price": round(vpn_hourly * HOURS_PER_YEAR, 4),
    }

    # NAT IP pricing
    nat_hourly = 0.004  # default fallback
    for sku in filtered.get("nat_ip", []):
        price = _extract_hourly_price(sku)
        if price > 0:
            nat_hourly = price
            break

    catalog["nat_ip"] = {
        "region": region,
        "description": "Cloud NAT IP address",
        "hourly_price": nat_hourly,
        "monthly_price": round(nat_hourly * HOURS_PER_MONTH, 4),
        "annual_price": round(nat_hourly * HOURS_PER_YEAR, 4),
    }

    # Interconnect VLAN attachment — billed by Interconnect, not router
    catalog["interconnect_vlan"] = {
        "region": region,
        "description": "Interconnect VLAN attachment (billed by Interconnect, not Cloud Router)",
        "hourly_price": 0.0,
        "monthly_price": 0.0,
        "annual_price": 0.0,
    }

    return catalog


def main():
    if len(sys.argv) < 2:
        print("Usage: python cr_cloudrouter_fetch.py <region> [output_file]")
        print("\nExample: python cr_cloudrouter_fetch.py us-central1")
        sys.exit(1)

    region = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        os.path.dirname(__file__), "..", "skus", "cr_cloudrouter_sku.json"
    )

    print(f"\n=== Building Cloud Router SKU catalog for region: {region} ===\n")

    all_skus = fetch_all_skus()
    print(f"\nTotal SKUs fetched: {len(all_skus)}")

    filtered = filter_cloudrouter_skus(all_skus, region)
    print(f"VPN tunnel SKUs: {len(filtered['vpn_tunnel'])}")
    print(f"NAT IP SKUs    : {len(filtered['nat_ip'])}")

    catalog = build_cloudrouter_catalog(filtered, region)

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"\n=== Summary ===")
    print(f"VPN tunnel hourly : ${catalog['vpn_tunnel']['hourly_price']}")
    print(f"NAT IP hourly     : ${catalog['nat_ip']['hourly_price']}")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    main()
