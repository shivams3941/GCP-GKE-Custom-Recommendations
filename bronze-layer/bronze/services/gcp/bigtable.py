"""GCP Bigtable service definition — pure config, no logic.

Declares how to fetch Bigtable instances, clusters, and tables from GCP,
and which Iceberg tables to write them to.

Notes:
- resource_id for instances: "project_id.instance_id"
- resource_id for clusters:  "project_id.instance_id.cluster_id"
- resource_id for tables:    "project_id.instance_id.table_id"
- Clusters and tables are fetched per instance.
- Metrics are project-level under the bigtable.googleapis.com namespace.

Idle signals:
  bigtable/server/request_count, bigtable/cluster/cpu_load

Overprovisioned signals:
  bigtable/cluster/cpu_load, bigtable/cluster/node_count
"""

from google.cloud import bigtable
from google.cloud.bigtable import instance as bt_instance

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

BT_INSTANCES_TABLE = TableConfig(
    table_name="bronze_gcp_bigtable_instances",
    s3_path_suffix="gcp/bigtable_instances",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.instance_id
        "resource_name": "string",
        "project_id": "string",
        "instance_id": "string",
        "display_name": "string",
        "instance_type": "string",          # PRODUCTION, DEVELOPMENT
        "state": "string",                  # READY, CREATING
        "labels": "map<string,string>",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

BT_CLUSTERS_TABLE = TableConfig(
    table_name="bronze_gcp_bigtable_clusters",
    s3_path_suffix="gcp/bigtable_clusters",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.instance_id.cluster_id
        "resource_name": "string",
        "project_id": "string",
        "instance_id": "string",
        "cluster_id": "string",
        "location": "string",               # zone, e.g. us-central1-a
        "state": "string",                  # READY, CREATING, RESIZING, DISABLED
        "serve_nodes": "double",
        "storage_type": "string",           # SSD, HDD
        "autoscaling_enabled": "string",    # "true" / "false"
        "autoscaling_min_nodes": "double",
        "autoscaling_max_nodes": "double",
        "autoscaling_cpu_target": "double",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

BT_TABLES_TABLE = TableConfig(
    table_name="bronze_gcp_bigtable_tables",
    s3_path_suffix="gcp/bigtable_tables",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.instance_id.table_id
        "resource_name": "string",
        "project_id": "string",
        "instance_id": "string",
        "table_id": "string",
        "replication_state": "string",
        "column_families": "string",        # comma-separated family names
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

BT_METRICS_TABLE = TableConfig(
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
# Field mappings: Bigtable SDK attribute → output column name
# ---------------------------------------------------------------------------

BT_INSTANCE_FIELD_MAPPING = {
    "instance_id": "instance_id",
    "display_name": "display_name",
    "type_": "instance_type",
    "state": "state",
    "labels": "labels",
}

BT_CLUSTER_FIELD_MAPPING = {
    "cluster_id": "cluster_id",
    "location_id": "location",
    "state": "state",
    "serve_nodes": "serve_nodes",
    "default_storage_type": "storage_type",
}

BT_TABLE_FIELD_MAPPING = {
    "table_id": "table_id",
    "replication_state": "replication_state",
    "column_families": "column_families",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

BIGTABLE_SERVICE = ServiceDefinition(
    name="BIGTABLE",
    namespace="bigtable.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=bigtable.Client,
            list_method="list_instances",
            field_mapping=BT_INSTANCE_FIELD_MAPPING,
            table_config=BT_INSTANCES_TABLE,
            composite_id_fields=("project_id", "instance_id"),
        ),
        ResourceFetcher(
            sdk_client_class=bigtable.Client,
            list_method="list_clusters",
            field_mapping=BT_CLUSTER_FIELD_MAPPING,
            table_config=BT_CLUSTERS_TABLE,
            parent_id_source="instance_id",
            paginated=False,
            composite_id_fields=("project_id", "instance_id", "cluster_id"),
        ),
        ResourceFetcher(
            sdk_client_class=bigtable.Client,
            list_method="list_tables",
            field_mapping=BT_TABLE_FIELD_MAPPING,
            table_config=BT_TABLES_TABLE,
            parent_id_source="instance_id",
            paginated=False,
            composite_id_fields=("project_id", "instance_id", "table_id"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Idle signals
            MetricSpec("bigtable.googleapis.com/server/request_count", unit="Count", aggregation="Average", interval="PT5M"),
            MetricSpec("bigtable.googleapis.com/cluster/cpu_load", unit="Ratio", aggregation="Average", interval="PT5M"),
            MetricSpec("bigtable.googleapis.com/server/latencies", unit="ms", aggregation="Average", interval="PT5M"),
            MetricSpec("bigtable.googleapis.com/disk/bytes_used", unit="Bytes", aggregation="Average", interval="PT5M"),
            # Overprovisioned signals
            MetricSpec("bigtable.googleapis.com/cluster/node_count", unit="Count", aggregation="Average", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=BT_METRICS_TABLE,
    ),
)
