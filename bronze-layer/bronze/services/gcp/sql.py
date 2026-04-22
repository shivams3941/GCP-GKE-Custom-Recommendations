"""GCP Cloud SQL service definition — pure config, no logic.

Declares how to fetch Google Cloud SQL Instances and metrics,
and which Iceberg tables to write them to.
"""

from google.cloud import sql_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

SQL_INSTANCES_TABLE = TableConfig(
    table_name="bronze_gcp_sql_instances",
    s3_path_suffix="gcp/sql_instances",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "database_version": "string",
        "region": "string",
        "gce_zone": "string",
        "state": "string",
        "tier": "string",
        "availability_type": "string",
        "disk_type": "string",
        "data_disk_size_gb": "int",
        "settings_pricing_plan": "string",
        "server_ca_cert": "string",
        "ip_addresses": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

SQL_METRICS_TABLE = TableConfig(
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

SQL_INSTANCE_FIELD_MAPPING = {
    "name": "resource_id",
    "name": "resource_name",
    "database_version": "database_version",
    "region": "region",
    "gce_zone": "gce_zone",
    "state": "state",
    "settings.tier": "tier",
    "settings.availability_type": "availability_type",
    "settings.data_disk_type": "disk_type",
    "settings.data_disk_size_gb": "data_disk_size_gb",
    "settings.pricing_plan": "settings_pricing_plan",
    "server_ca_cert.cert_serial_number": "server_ca_cert",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_SQL_SERVICE = ServiceDefinition(
    name="CloudSQL",
    namespace="cloudsql.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=sql_v1.SqlInstancesServiceClient,
            list_method="list",
            field_mapping=SQL_INSTANCE_FIELD_MAPPING,
            table_config=SQL_INSTANCES_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            MetricSpec("cloudsql.googleapis.com/database/cpu/utilization", unit="Percent", aggregation="Average", interval="PT5M"),
            MetricSpec("cloudsql.googleapis.com/database/memory/utilization", unit="Percent", aggregation="Average", interval="PT5M"),
            MetricSpec("cloudsql.googleapis.com/database/disk/utilization", unit="Percent", aggregation="Average", interval="PT5M"),
            MetricSpec("cloudsql.googleapis.com/database/disk/read_ops_count", unit="Count", aggregation="Average", interval="PT5M"),
            MetricSpec("cloudsql.googleapis.com/database/disk/write_ops_count", unit="Count", aggregation="Average", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=SQL_METRICS_TABLE,
    ),
)
