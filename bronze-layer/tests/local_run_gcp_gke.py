"""
Local runner for GKE service — fetches real data from GCP and saves to CSV.
Validates clusters, node pools, and Cloud Monitoring metrics.

Usage (from project root):
    python tests/local_run_gcp_gke.py

Output:
    tests/output/gke_output.csv
"""

import csv
import os
import sys
from datetime import datetime, timedelta, timezone

# Ensure project root is on the path so bronze package is found
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from google.cloud import container_v1, monitoring_v3
from google.cloud.container_v1.types import Cluster, NodePool

from bronze.auth.gcp_auth import get_gcp_credentials
from bronze.services.gcp.gke import (
    GKE_CLUSTER_FIELD_MAPPING,
    GKE_NODE_POOL_FIELD_MAPPING,
    GKE_SERVICE,
)

# Enum resolvers
CLUSTER_STATUS = {v.value: k for k, v in Cluster.Status.__members__.items()}
NODE_POOL_STATUS = {v.value: k for k, v in NodePool.Status.__members__.items()}
RELEASE_CHANNEL = {
    0: "UNSPECIFIED", 1: "RAPID", 2: "REGULAR", 3: "STABLE", 4: "EXTENDED"
}

# --- Edit these before running ---
PROJECT_ID = "devops-internal-439011"
METRICS_WINDOW_DAYS = 7
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "gke_output.csv")


def _resolve_attr(obj, dot_path: str):
    current = obj
    for segment in dot_path.split("."):
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            current = getattr(current, segment, None)
    return current


def fetch_cluster_metrics(monitoring_client, location: str, cluster_name: str) -> list:
    """Fetch all GKE metrics from Cloud Monitoring for one cluster. Returns list of metric dicts."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=METRICS_WINDOW_DAYS)

    interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

    # Deduplicate specs by (metric_name, aggregation, percentile)
    seen = set()
    unique_specs = []
    for spec in GKE_SERVICE.metrics.metric_specs:
        key = (spec.metric_name, spec.aggregation, getattr(spec, "percentile", None))
        if key not in seen:
            seen.add(key)
            unique_specs.append(spec)

    metrics_output = []

    for spec in unique_specs:
        percentile = getattr(spec, "percentile", None)
        if spec.aggregation == "Percentile" and percentile == 99:
            reducer = monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99
        else:
            reducer = monitoring_v3.Aggregation.Reducer.REDUCE_MEAN

        aggregation = monitoring_v3.Aggregation(
            alignment_period={"seconds": 300},
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            cross_series_reducer=reducer,
            group_by_fields=["resource.labels.cluster_name"],
        )

        metric_entry = {
            "metric_name": spec.metric_name,
            "aggregation": spec.aggregation,
            "percentile": percentile,
            "unit": spec.unit,
            "interval": spec.interval,
            "data_points": [],
            "error": None,
        }

        try:
            results = monitoring_client.list_time_series(
                request={
                    "name": f"projects/{PROJECT_ID}",
                    "filter": (
                        f'metric.type = "{spec.metric_name}" '
                        f'AND resource.labels.cluster_name = "{cluster_name}" '
                        f'AND resource.labels.location = "{location}"'
                    ),
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    "aggregation": aggregation,
                }
            )

            for ts in results:
                for point in ts.points:
                    pt = point.interval.end_time
                    if hasattr(pt, "ToDatetime"):
                        pt = pt.ToDatetime(tzinfo=timezone.utc)
                    value = point.value.double_value or point.value.int64_value or 0.0
                    metric_entry["data_points"].append({
                        "timestamp": pt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "value": round(float(value), 6),
                    })

        except Exception as e:
            metric_entry["error"] = str(e)

        metrics_output.append(metric_entry)

    return metrics_output


def run():
    print("Loading GCP credentials...")
    credentials = get_gcp_credentials()
    print(f"Authenticated as: {credentials.service_account_email}")

    gke_client = container_v1.ClusterManagerClient(credentials=credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)

    print(f"Fetching GKE clusters for project: {PROJECT_ID}...")
    response = gke_client.list_clusters(parent=f"projects/{PROJECT_ID}/locations/-")
    clusters = list(response.clusters)
    print(f"Found {len(clusters)} cluster(s)")

    output = {
        "project_id": PROJECT_ID,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metrics_window_days": METRICS_WINDOW_DAYS,
        "clusters": [],
    }

    for cluster in clusters:
        # Map cluster fields
        record = {}
        for sdk_path, output_field in GKE_CLUSTER_FIELD_MAPPING.items():
            record[output_field] = _resolve_attr(cluster, sdk_path)

        location = cluster.location
        cluster_name = cluster.name
        resource_id = f"{PROJECT_ID}.{location}.{cluster_name}"

        cluster_entry = {
            "resource_id": resource_id,
            "cluster_name": cluster_name,
            "project_id": PROJECT_ID,
            "location": location,
            "status": CLUSTER_STATUS.get(record.get("status"), str(record.get("status"))),
            "current_master_version": record.get("current_master_version"),
            "current_node_version": record.get("current_node_version"),
            "current_node_count": record.get("current_node_count"),
            "autopilot_enabled": str(record.get("autopilot_enabled")),
            "release_channel": RELEASE_CHANNEL.get(record.get("release_channel"), str(record.get("release_channel"))),
            "network": record.get("network"),
            "subnetwork": record.get("subnetwork"),
            "logging_service": record.get("logging_service"),
            "monitoring_service": record.get("monitoring_service"),
            "endpoint": record.get("endpoint"),
            "node_pools": [],
            "metrics": [],
        }

        # Fetch node pools
        cluster_parent = f"projects/{PROJECT_ID}/locations/{location}/clusters/{cluster_name}"
        np_response = gke_client.list_node_pools(parent=cluster_parent)

        for np in np_response.node_pools:
            np_record = {}
            for sdk_path, output_field in GKE_NODE_POOL_FIELD_MAPPING.items():
                np_record[output_field] = _resolve_attr(np, sdk_path)

            cluster_entry["node_pools"].append({
                "resource_id": f"{resource_id}.{np.name}",
                "node_pool_name": np.name,
                "status": NODE_POOL_STATUS.get(np_record.get("status"), str(np_record.get("status"))),
                "version": np_record.get("version"),
                "machine_type": np_record.get("machine_type"),
                "disk_size_gb": np_record.get("disk_size_gb"),
                "disk_type": np_record.get("disk_type"),
                "image_type": np_record.get("image_type"),
                "preemptible": str(np_record.get("preemptible")),
                "spot": str(np_record.get("spot")),
                "initial_node_count": np_record.get("initial_node_count"),
                "autoscaling_enabled": str(np_record.get("autoscaling_enabled")),
                "min_node_count": np_record.get("min_node_count"),
                "max_node_count": np_record.get("max_node_count"),
                "locations": np_record.get("locations"),
            })

        # Fetch all metrics
        print(f"  Fetching metrics for cluster: {cluster_name}...")
        cluster_entry["metrics"] = fetch_cluster_metrics(monitoring_client, location, cluster_name)

        output["clusters"].append(cluster_entry)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    CSV_FIELDS = [
        # cluster
        "project_id", "fetched_at", "resource_id", "cluster_name", "location", "status",
        "current_master_version", "current_node_version", "current_node_count",
        "autopilot_enabled", "release_channel", "network", "subnetwork",
        "logging_service", "monitoring_service", "endpoint",
        # node pool
        "node_pool_resource_id", "node_pool_name", "node_pool_status", "node_pool_version",
        "machine_type", "disk_size_gb", "disk_type", "image_type",
        "preemptible", "spot", "initial_node_count",
        "autoscaling_enabled", "min_node_count", "max_node_count", "locations",
        # metric
        "metric_name", "aggregation", "percentile", "unit", "interval",
        "metric_timestamp", "metric_value", "metric_error",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for c in output["clusters"]:
            cluster_base = {
                "project_id": output["project_id"],
                "fetched_at": output["fetched_at"],
                "resource_id": c["resource_id"],
                "cluster_name": c["cluster_name"],
                "location": c["location"],
                "status": c["status"],
                "current_master_version": c["current_master_version"],
                "current_node_version": c["current_node_version"],
                "current_node_count": c["current_node_count"],
                "autopilot_enabled": c["autopilot_enabled"],
                "release_channel": c["release_channel"],
                "network": c["network"],
                "subnetwork": c["subnetwork"],
                "logging_service": c["logging_service"],
                "monitoring_service": c["monitoring_service"],
                "endpoint": c["endpoint"],
            }

            # Build a lookup: node_pool_name -> node_pool dict
            np_lookup = {np["node_pool_name"]: np for np in c["node_pools"]}
            # Use first node pool as default if only one, else leave blank for metrics rows
            default_np = c["node_pools"][0] if len(c["node_pools"]) == 1 else {}

            for m in c["metrics"]:
                np = np_lookup.get(m.get("node_pool_name"), default_np)
                np_fields = {
                    "node_pool_resource_id": np.get("resource_id"),
                    "node_pool_name": np.get("node_pool_name"),
                    "node_pool_status": np.get("status"),
                    "node_pool_version": np.get("version"),
                    "machine_type": np.get("machine_type"),
                    "disk_size_gb": np.get("disk_size_gb"),
                    "disk_type": np.get("disk_type"),
                    "image_type": np.get("image_type"),
                    "preemptible": np.get("preemptible"),
                    "spot": np.get("spot"),
                    "initial_node_count": np.get("initial_node_count"),
                    "autoscaling_enabled": np.get("autoscaling_enabled"),
                    "min_node_count": np.get("min_node_count"),
                    "max_node_count": np.get("max_node_count"),
                    "locations": np.get("locations"),
                }

                if m["data_points"]:
                    for dp in m["data_points"]:
                        writer.writerow({
                            **cluster_base, **np_fields,
                            "metric_name": m["metric_name"],
                            "aggregation": m["aggregation"],
                            "percentile": m["percentile"],
                            "unit": m["unit"],
                            "interval": m["interval"],
                            "metric_timestamp": dp["timestamp"],
                            "metric_value": dp["value"],
                            "metric_error": m["error"],
                        })
                else:
                    writer.writerow({
                        **cluster_base, **np_fields,
                        "metric_name": m["metric_name"],
                        "aggregation": m["aggregation"],
                        "percentile": m["percentile"],
                        "unit": m["unit"],
                        "interval": m["interval"],
                        "metric_timestamp": None,
                        "metric_value": None,
                        "metric_error": m["error"],
                    })

    print(f"\nOutput saved to: {OUTPUT_CSV}")
    print("Done.")


if __name__ == "__main__":
    run()
