"""
Local development / test script for the GKE rightsizing engine.

Reads real cluster data from bronze-layer CSV output and runs the engine
against each cluster row, printing recommendations to stdout.

Usage (from gold-layer/gcp/):
    python _local_dev.py

Prerequisites:
    pip install -r ../../bronze-layer/requirements.txt   # or just: pip install requests
    The bronze CSV must exist at:
        ../../bronze-layer/tests/output/gke_output.csv
"""

import csv
import json
import logging
import os
import sys

# ---------------------------------------------------------------------------
# Path setup — allow importing rightsize_engine from this directory
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))

from rightsize_engine import GKEEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("local_dev")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SKU_PATH = os.path.join(os.path.dirname(__file__), "resources", "skus")
SKU_FILE = "cr_gke_sku.json"

BRONZE_CSV = os.path.join(
    os.path.dirname(__file__), "..", "..", "bronze-layer", "tests", "output", "gke_output.csv"
)

# Thresholds (mirrors what would be in finops_threshold_rules table)
RULES = [
    {
        "rule_code": "GKE_IDLE",
        "description": "Node pool CPU avg < 5% AND memory avg < 20% — likely idle",
        "recommendation_template": "Idle GKE node pool — delete or scale to 0 nodes (current: {current_sku})",
        "conditions": [
            {"metric": "cpu_utilization_avg",    "operator": "lt", "threshold": 5.0},
            {"metric": "memory_utilization_avg", "operator": "lt", "threshold": 20.0},
        ],
        "logic": "AND",
    },
    {
        "rule_code": "GKE_OVERPROVISIONED",
        "description": "Node pool CPU avg < 40% OR memory avg < 50% — likely overprovisioned",
        "recommendation_template": "Overprovisioned GKE node pool — rightsize from {current_sku} to {target_sku}",
        "conditions": [
            {"metric": "cpu_utilization_avg",    "operator": "lt", "threshold": 40.0},
            {"metric": "memory_utilization_avg", "operator": "lt", "threshold": 50.0},
        ],
        "logic": "OR",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0) -> float:
    try:
        return float(val) if val not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def _evaluate_rule(rule: dict, metrics: dict) -> bool:
    """Simple in-process rule evaluator (mirrors ThresholdRule.evaluate)."""
    conditions = rule["conditions"]
    logic = rule.get("logic", "AND").upper()
    results = []

    for cond in conditions:
        key = cond["metric"]
        op = cond["operator"]
        threshold = float(cond["threshold"])
        value = _safe_float(metrics.get(key))

        if op == "lt":
            results.append(value < threshold)
        elif op == "lte":
            results.append(value <= threshold)
        elif op == "gt":
            results.append(value > threshold)
        elif op == "gte":
            results.append(value >= threshold)
        elif op == "eq":
            results.append(abs(value - threshold) < 0.001)

    if not results:
        return False
    return all(results) if logic == "AND" else any(results)


def load_bronze_csv(path: str) -> list:
    """Load the bronze GKE CSV and return a list of row dicts."""
    if not os.path.exists(path):
        logger.error(f"Bronze CSV not found: {path}")
        logger.error("Run bronze-layer/tests/local_run_gcp_gke.py first to generate it.")
        sys.exit(1)

    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    logger.info(f"Loaded {len(rows)} rows from {path}")
    return rows


def aggregate_metrics_per_cluster(rows: list) -> dict:
    """
    Aggregate metric values per cluster from the flat CSV.
    Returns: { cluster_name: { metric_name: avg_value, ... } }
    """
    from collections import defaultdict

    # { cluster_name: { metric_name: [values] } }
    raw: dict = defaultdict(lambda: defaultdict(list))

    for row in rows:
        cluster = row.get("cluster_name", "")
        metric = row.get("metric_name", "")
        value = _safe_float(row.get("metric_value"))
        if cluster and metric and value > 0:
            raw[cluster][metric].append(value)

    # Compute averages and map to engine metric keys
    METRIC_MAP = {
        "kubernetes.io/node/cpu/allocatable_utilization":    "cpu_utilization_avg",
        "kubernetes.io/node/memory/allocatable_utilization": "memory_utilization_avg",
        "kubernetes.io/container/cpu/request_utilization":   "container_cpu_avg",
        "kubernetes.io/container/memory/request_utilization":"container_memory_avg",
    }

    aggregated = {}
    for cluster, metrics in raw.items():
        agg = {}
        for gcp_metric, engine_key in METRIC_MAP.items():
            values = metrics.get(gcp_metric, [])
            if values:
                avg = sum(values) / len(values)
                # GCP returns ratio (0-1), convert to percentage
                agg[engine_key] = round(avg * 100, 4)
        aggregated[cluster] = agg

    return aggregated


def build_cluster_resource_data(rows: list) -> dict:
    """
    Extract cluster-level resource data from the first row per cluster.
    Returns: { cluster_name: { resource fields } }
    """
    seen = {}
    for row in rows:
        cluster = row.get("cluster_name", "")
        if cluster and cluster not in seen:
            seen[cluster] = {
                "cluster_name":          cluster,
                "resource_id":           row.get("resource_id", ""),
                "project_id":            row.get("project_id", ""),
                "location":              row.get("location", ""),
                "status":                row.get("status", ""),
                "current_node_count":    _safe_float(row.get("current_node_count"), 1),
                "machine_type":          row.get("machine_type", "e2-medium"),
                "node_pool_name":        row.get("node_pool_name", ""),
                "autopilot_enabled":     row.get("autopilot_enabled", "False"),
                "release_channel":       row.get("release_channel", ""),
            }
    return seen


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n" + "=" * 60)
    print("GKE Rightsizing Engine — Local Test")
    print("=" * 60)

    # Init engine
    engine = GKEEngine(sku_path=SKU_PATH, json_filename=SKU_FILE)
    print(f"\nSKU catalog loaded: {len(engine.sku_catalog)} machine families")

    # Load bronze data
    rows = load_bronze_csv(BRONZE_CSV)

    # Aggregate
    metrics_by_cluster = aggregate_metrics_per_cluster(rows)
    resource_by_cluster = build_cluster_resource_data(rows)

    print(f"\nClusters found: {list(resource_by_cluster.keys())}")
    print(f"Clusters with metrics: {list(metrics_by_cluster.keys())}")

    all_recommendations = []

    for cluster_name, resource_data in resource_by_cluster.items():
        metrics = metrics_by_cluster.get(cluster_name, {})
        machine_type = resource_data.get("machine_type", "e2-medium")
        region = resource_data.get("location", "us-central1")
        # GKE location can be a zone (us-central1-a) — strip to region
        if region.count("-") == 2:
            region = "-".join(region.split("-")[:2])

        print(f"\n{'─'*50}")
        print(f"Cluster : {cluster_name}")
        print(f"Location: {region}  |  Machine: {machine_type}  |  Nodes: {resource_data.get('current_node_count')}")
        print(f"Metrics : {metrics}")

        if not metrics:
            print("  WARNING: No metrics available — skipping")
            continue

        triggered = False
        for rule in RULES:
            if not _evaluate_rule(rule, metrics):
                continue

            triggered = True
            print(f"\n  Rule triggered: {rule['rule_code']}")
            print(f"  {rule['description']}")

            result = engine.find_rightsize_candidate(
                current_sku=machine_type,
                region=region,
                rule_code=rule["rule_code"],
                recommendation_template=rule["recommendation_template"],
                metrics=metrics,
                resource_data=resource_data,
            )

            if result:
                rec_text, current_annual, target_annual, savings, details = result
                print(f"\n  Recommendation : {rec_text}")
                print(f"  Current annual : ${current_annual:,.2f}")
                print(f"  Target annual  : ${target_annual:,.2f}")
                print(f"  Annual savings : ${savings:,.2f}")
                if details.get("target_skus"):
                    for t in details["target_skus"]:
                        print(f"  Target SKU     : {t['machine_type']} ({t['vcpus']} vCPU, {t['ram_gb']}GB RAM)")
                all_recommendations.append({
                    "cluster_name": cluster_name,
                    "rule_code": rule["rule_code"],
                    "recommendation": rec_text,
                    "current_annual": current_annual,
                    "target_annual": target_annual,
                    "annual_savings": savings,
                    "details": details,
                })
            else:
                print("  INFO: Engine returned no candidate (machine type may not be in catalog)")

        if not triggered:
            print("  OK: No rules triggered — cluster looks healthy")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total recommendations : {len(all_recommendations)}")
    total_savings = sum(r["annual_savings"] for r in all_recommendations)
    print(f"Total annual savings  : ${total_savings:,.2f}")

    for r in all_recommendations:
        print(f"  [{r['rule_code']}] {r['cluster_name']} — save ${r['annual_savings']:,.2f}/yr")

    print()


if __name__ == "__main__":
    main()
