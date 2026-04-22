"""GCP BigQuery service definition — pure config, no logic.

Declares how to fetch BigQuery datasets and tables from GCP,
and which Iceberg tables to write them to.

Notes:
- resource_id for datasets: "project_id.dataset_id"
- resource_id for tables:   "project_id.dataset_id.table_id"
- Metrics are project-level under the bigquery.googleapis.com namespace.
- Tables are fetched per dataset (list_tables requires dataset_id).
  The framework iterates datasets first, then calls list_tables per dataset.

Idle signals:
  bigquery/query/count, bigquery/storage/stored_bytes

Overprovisioned signals:
  bigquery/slots/allocated_for_project, bigquery/query/scanned_bytes_billed
"""

from google.cloud import bigquery as bq_sdk

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

BQ_DATASETS_TABLE = TableConfig(
    table_name="bronze_gcp_bigquery_datasets",
    s3_path_suffix="gcp/bigquery_datasets",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.dataset_id
        "resource_name": "string",
        "project_id": "string",
        "dataset_id": "string",
        "friendly_name": "string",
        "description": "string",
        "location": "string",
        "default_table_expiration_ms": "double",
        "default_partition_expiration_ms": "double",
        "labels": "map<string,string>",
        "created": "string",
        "modified": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

BQ_TABLES_TABLE = TableConfig(
    table_name="bronze_gcp_bigquery_tables",
    s3_path_suffix="gcp/bigquery_tables",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.dataset_id.table_id
        "resource_name": "string",
        "project_id": "string",
        "dataset_id": "string",
        "table_id": "string",
        "table_type": "string",             # TABLE, VIEW, EXTERNAL, etc.
        "friendly_name": "string",
        "description": "string",
        "num_bytes": "double",
        "num_rows": "double",
        "created": "string",
        "modified": "string",
        "expires": "string",
        "partitioning_type": "string",      # DAY, HOUR, MONTH, YEAR, RANGE
        "partition_field": "string",
        "clustering_fields": "string",      # comma-separated
        "labels": "map<string,string>",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

BQ_METRICS_TABLE = TableConfig(
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
# Field mappings: BQ SDK attribute → output column name
# ---------------------------------------------------------------------------

BQ_DATASET_FIELD_MAPPING = {
    "dataset_id": "dataset_id",
    "friendly_name": "friendly_name",
    "description": "description",
    "location": "location",
    "default_table_expiration_ms": "default_table_expiration_ms",
    "default_partition_expiration_ms": "default_partition_expiration_ms",
    "labels": "labels",
    "created": "created",
    "modified": "modified",
}

BQ_TABLE_FIELD_MAPPING = {
    "table_id": "table_id",
    "table_type": "table_type",
    "friendly_name": "friendly_name",
    "description": "description",
    "num_bytes": "num_bytes",
    "num_rows": "num_rows",
    "created": "created",
    "modified": "modified",
    "expires": "expires",
    "partitioning_type": "partitioning_type",
    "partition_field": "partition_field",
    "clustering_fields": "clustering_fields",
    "labels": "labels",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

BIGQUERY_SERVICE = ServiceDefinition(
    name="BIGQUERY",
    namespace="bigquery.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=bq_sdk.Client,
            list_method="list_datasets",
            field_mapping=BQ_DATASET_FIELD_MAPPING,
            table_config=BQ_DATASETS_TABLE,
            composite_id_fields=("project_id", "dataset_id"),
        ),
        ResourceFetcher(
            sdk_client_class=bq_sdk.Client,
            list_method="list_tables",
            field_mapping=BQ_TABLE_FIELD_MAPPING,
            table_config=BQ_TABLES_TABLE,
            parent_id_source="dataset_id",
            paginated=True,
            composite_id_fields=("project_id", "dataset_id", "table_id"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # --- Dataset-scoped (resource: bigquery_dataset) ---
            # Idle signals
            MetricSpec("bigquery.googleapis.com/storage/stored_bytes", unit="Bytes", aggregation="Average", interval="PT1H"),
            MetricSpec("bigquery.googleapis.com/storage/table_count", unit="Count", aggregation="Average", interval="PT1H"),
            # --- Project-scoped (resource: bigquery_project) ---
            # Idle signal — primary query activity indicator
            MetricSpec("bigquery.googleapis.com/query/count", unit="Count", aggregation="Average", interval="PT1H"),
            # Overprovisioned signal
            MetricSpec("bigquery.googleapis.com/slots/allocated_for_project", unit="Count", aggregation="Average", interval="PT1H"),
            # --- Global-scoped (resource: global) ---
            # Overprovisioned signal — billed bytes scanned (missing partition/cluster filters)
            MetricSpec("bigquery.googleapis.com/query/scanned_bytes_billed", unit="Bytes", aggregation="Average", interval="PT1H"),
        ],
        resource_id_field="resource_id",
        table_config=BQ_METRICS_TABLE,
    ),
)
