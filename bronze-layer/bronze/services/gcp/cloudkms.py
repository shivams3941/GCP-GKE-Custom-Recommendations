"""GCP Cloud KMS service definition — pure config, no logic.

Declares how to fetch Google Cloud KMS Key Rings and Crypto Keys,
and which Iceberg tables to write them to.
"""

from google.cloud import kms_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

KMS_KEY_RINGS_TABLE = TableConfig(
    table_name="bronze_gcp_kms_key_rings",
    s3_path_suffix="gcp/kms_key_rings",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "create_time": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

KMS_CRYPTO_KEYS_TABLE = TableConfig(
    table_name="bronze_gcp_kms_crypto_keys",
    s3_path_suffix="gcp/kms_crypto_keys",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "key_ring_id": "string",
        "purpose": "string",
        "create_time": "string",
        "next_rotation_time": "string",
        "rotation_period": "string",
        "labels": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

KMS_METRICS_TABLE = TableConfig(
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

KEY_RING_FIELD_MAPPING = {
    "name": "resource_id", # Full path serves as ID
    "name": "resource_name",
    "create_time": "create_time",
}

CRYPTO_KEY_FIELD_MAPPING = {
    "name": "resource_id", # Full path serves as ID
    "name": "resource_name",
    "purpose": "purpose",
    "create_time": "create_time",
    "next_rotation_time": "next_rotation_time",
    "rotation_period": "rotation_period",
    "labels": "labels",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_KMS_SERVICE = ServiceDefinition(
    name="CloudKMS",
    namespace="cloudkms.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=kms_v1.KeyManagementServiceClient,
            list_method="list_key_rings",
            fallback_list_method="list_key_rings",
            field_mapping=KEY_RING_FIELD_MAPPING,
            table_config=KMS_KEY_RINGS_TABLE,
            # Requires parent location, e.g. parent="projects/{project_id}/locations/{location}"
        ),
        ResourceFetcher(
            sdk_client_class=kms_v1.KeyManagementServiceClient,
            list_method="list_crypto_keys",
            fallback_list_method="list_crypto_keys",
            field_mapping=CRYPTO_KEY_FIELD_MAPPING,
            table_config=KMS_CRYPTO_KEYS_TABLE,
            parent_id_source="resource_name", # Iterate from Key Rings
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            MetricSpec("serviceruntime.googleapis.com/api/request_count", unit="Count", aggregation="Average", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=KMS_METRICS_TABLE,
    ),
)
