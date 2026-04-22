"""GCP GKE service definition — pure config, no logic.

Declares how to fetch GKE clusters and node pools from GCP,
and which Iceberg tables to write them to.

Authentication:
- GCP credentials are loaded at runtime via bronze.auth.gcp_auth.get_gcp_credentials()
- The service account key JSON is stored in AWS Secrets Manager under
  the secret name defined in GCP_SECRET_NAME (gcp/devops-internal/service-account)
- The runner is responsible for passing credentials to the SDK client,
  not this service definition file.

Notes:
- Node pools are fetched per cluster (list_node_pools requires cluster name).
  The framework must iterate over clusters first, then call list_node_pools
  for each cluster — see parent_id_source on the node pools ResourceFetcher.
- resource_id for clusters is composed as "project_id.location.cluster_name"
  for a clean, unambiguous primary key across multi-project/multi-region setups.
- resource_id for node pools is composed as
  "project_id.location.cluster_name.node_pool_name" for full lineage traceability.
- Metrics are project-level under the kubernetes.io namespace; they are keyed
  by cluster, node pool, namespace, and container resource dimensions in
  Cloud Monitoring.
"""

from google.cloud import container_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

GKE_CLUSTERS_TABLE = TableConfig(
    table_name="bronze_gcp_gke_clusters",
    s3_path_suffix="gcp/gke_clusters",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.location.cluster_name
        "resource_name": "string",          # cluster display name
        "project_id": "string",
        "location": "string",               # region or zone (e.g. us-central1 or us-central1-a)
        "cluster_name": "string",
        "description": "string",
        "current_master_version": "string",
        "current_node_version": "string",
        "current_node_count": "double",
        "status": "string",
        "autopilot_enabled": "string",      # "true" / "false" string flag
        "network": "string",
        "subnetwork": "string",
        "cluster_ipv4_cidr": "string",
        "services_ipv4_cidr": "string",
        "logging_service": "string",
        "monitoring_service": "string",
        "release_channel": "string",        # RAPID, REGULAR, STABLE, UNSPECIFIED
        "labels": "map<string,string>",
        "create_time": "string",
        "expire_time": "string",
        "endpoint": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

GKE_NODE_POOLS_TABLE = TableConfig(
    table_name="bronze_gcp_gke_node_pools",
    s3_path_suffix="gcp/gke_node_pools",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.location.cluster_name.node_pool_name
        "resource_name": "string",
        "project_id": "string",
        "location": "string",
        "cluster_name": "string",
        "node_pool_name": "string",
        "status": "string",
        "version": "string",
        "machine_type": "string",
        "disk_size_gb": "double",
        "disk_type": "string",
        "image_type": "string",
        "preemptible": "string",            # "true" / "false"
        "spot": "string",                   # "true" / "false"
        "initial_node_count": "double",
        "autoscaling_enabled": "string",    # "true" / "false"
        "min_node_count": "double",
        "max_node_count": "double",
        "total_min_node_count": "double",
        "total_max_node_count": "double",
        "locations": "string",              # comma-separated zone list
        "labels": "map<string,string>",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

GKE_METRICS_TABLE = TableConfig(
    table_name="bronze_gcp_metrics_v2",
    s3_path_suffix="gcp/metrics",
    key_columns=("client_id", "account_id", "resource_id", "date", "metric_name", "aggregation_type"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "account_id": "string",
        "aggregation_type": "string",
        "client_id": "string",
        "cloud_name": "string",
        "date": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
        "metric_date": "string",
        "metric_name": "string",
        "metric_type": "string",
        "metric_unit": "string",
        "metric_value": "double",
        "namespace": "string",
        "region": "string",
        "resource_id": "string",
        "resource_name": "string",
        "service_name": "string",
        "unit": "string",
        "year_month": "string",
    },
)

# ---------------------------------------------------------------------------
# Field mappings: GCP SDK attribute dot-path → output column name
# ---------------------------------------------------------------------------

GKE_CLUSTER_FIELD_MAPPING = {
    "name": "cluster_name",
    # resource_name is derived from cluster_name at transform time (same source,
    # can't use duplicate dict key)
    "description": "description",
    "location": "location",
    "current_master_version": "current_master_version",
    "current_node_version": "current_node_version",
    "current_node_count": "current_node_count",
    "status": "status",
    "autopilot.enabled": "autopilot_enabled",
    "network": "network",
    "subnetwork": "subnetwork",
    "cluster_ipv4_cidr": "cluster_ipv4_cidr",
    "services_ipv4_cidr": "services_ipv4_cidr",
    "logging_service": "logging_service",
    "monitoring_service": "monitoring_service",
    "release_channel.channel": "release_channel",
    "resource_labels": "labels",
    "create_time": "create_time",
    "expire_time": "expire_time",
    "endpoint": "endpoint",
}

GKE_NODE_POOL_FIELD_MAPPING = {
    "name": "node_pool_name",
    # resource_name is derived from node_pool_name at transform time
    "status": "status",
    "version": "version",
    "config.machine_type": "machine_type",
    "config.disk_size_gb": "disk_size_gb",
    "config.disk_type": "disk_type",
    "config.image_type": "image_type",
    "config.preemptible": "preemptible",
    "config.spot": "spot",
    "config.labels": "labels",
    "initial_node_count": "initial_node_count",
    "autoscaling.enabled": "autoscaling_enabled",
    "autoscaling.min_node_count": "min_node_count",
    "autoscaling.max_node_count": "max_node_count",
    "autoscaling.total_min_node_count": "total_min_node_count",
    "autoscaling.total_max_node_count": "total_max_node_count",
    "locations": "locations",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

GKE_SERVICE = ServiceDefinition(
    name="GKE",
    namespace="kubernetes.io",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=container_v1.ClusterManagerClient,
            list_method="list_clusters",
            field_mapping=GKE_CLUSTER_FIELD_MAPPING,
            table_config=GKE_CLUSTERS_TABLE,
            composite_id_fields=("project_id", "location", "cluster_name"),
        ),
        ResourceFetcher(
            sdk_client_class=container_v1.ClusterManagerClient,
            list_method="list_node_pools",
            field_mapping=GKE_NODE_POOL_FIELD_MAPPING,
            table_config=GKE_NODE_POOLS_TABLE,
            parent_id_source="cluster_name",
            paginated=False,
            composite_id_fields=("project_id", "location", "cluster_name", "node_pool_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Idle detection
            MetricSpec("kubernetes.io/node/cpu/allocatable_utilization", unit="Ratio", aggregation="Average", interval="PT5M"),
            MetricSpec("kubernetes.io/node/memory/allocatable_utilization", unit="Ratio", aggregation="Average", interval="PT5M"),
            MetricSpec("kubernetes.io/container/cpu/request_utilization", unit="Ratio", aggregation="Average", interval="PT5M"),
            MetricSpec("kubernetes.io/container/memory/request_utilization", unit="Ratio", aggregation="Average", interval="PT5M"),
            MetricSpec("kubernetes.io/pod/volume/total_bytes", unit="Bytes", aggregation="Average", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=GKE_METRICS_TABLE,
    ),
)
