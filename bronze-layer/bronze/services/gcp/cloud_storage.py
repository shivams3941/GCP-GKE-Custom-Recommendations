"""GCP Cloud Storage service definition - pure config, no logic.

Declares how to fetch Cloud Storage buckets and metrics from GCP,
and which Iceberg tables to write them to.

Notes:
- Cloud Storage manages object storage buckets
- resource_id is composed as "project_id.bucket_name" for unique identification
- Metrics are collected from storage.googleapis.com namespace
- Idle signal: no API requests over 14-30 days
- Overprovisioned signal: high storage costs with low access patterns
"""

from google.cloud import storage_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUD_STORAGE_TABLE = TableConfig(
    table_name="bronze_gcp_cloud_storage",
    s3_path_suffix="gcp/cloud_storage",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.bucket_name
        "resource_name": "string",          # Bucket display name
        "project_id": "string",
        "bucket_name": "string",
        "location": "string",               # Bucket location (e.g., us-central1)
        "location_type": "string",          # MULTI_REGION, REGION, DUAL_REGION
        "storage_class": "string",          # STANDARD, NEARLINE, COLDLINE, ARCHIVE
        "time_created": "string",
        "updated": "string",
        "metageneration": "string",
        "storage_size_bytes": "double",
        "object_count": "int",
        "lifecycle_rules_count": "int",
        "labels": "map<string,string>",
        "default_event_based_hold": "boolean",
        "default_object_retention_days": "int",
        "retention_policy_enabled": "boolean",
        "retention_period_days": "int",
        " requester_pays": "boolean",
        "versioning_enabled": "boolean",
        "logging_enabled": "boolean",
        "website_config": "string",
        "cors_config": "string",
        "lifecycle_config": "string",
        "encryption_type": "string",        # Google-managed, Customer-managed, Customer-supplied
        "kms_key_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUD_STORAGE_METRICS_TABLE = TableConfig(
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
# Field mappings: GCP SDK attribute dot-path -> output column name
# ---------------------------------------------------------------------------

CLOUD_STORAGE_FIELD_MAPPING = {
    "name": "bucket_name",
    "location": "location",
    "storage_class": "storage_class",
    "time_created": "time_created",
    "updated": "updated",
    "metageneration": "metageneration",
    "labels": "labels",
    "default_event_based_hold": "default_event_based_hold",
    "retention_policy.retention_period": "retention_period_days",
    "billing.requester_pays": "requester_pays",
    "versioning.enabled": "versioning_enabled",
    "logging.log_object_prefix": "logging_enabled",
    "encryption.default_kms_key_name": "kms_key_name",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_STORAGE_SERVICE = ServiceDefinition(
    name="Cloud Storage",
    namespace="storage.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=storage_v1.BucketsClient,
            list_method="list_buckets",
            field_mapping=CLOUD_STORAGE_FIELD_MAPPING,
            table_config=CLOUD_STORAGE_TABLE,
            composite_id_fields=("project_id", "bucket_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Request metrics
            MetricSpec("storage.googleapis.com/api/request_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("storage.googleapis.com/storage/object_count", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("storage.googleapis.com/storage/total_bytes", unit="Bytes", aggregation="Gauge", interval="PT5M"),
            
            # Network metrics
            MetricSpec("storage.googleapis.com/network/sent_bytes_count", unit="Bytes", aggregation="Sum", interval="PT5M"),
            MetricSpec("storage.googleapis.com/network/received_bytes_count", unit="Bytes", aggregation="Sum", interval="PT5M"),
            
            # Availability metrics
            MetricSpec("storage.googleapis.com/storage/availability", unit="Ratio", aggregation="Mean", interval="PT5M"),
            
            # Error metrics
            MetricSpec("storage.googleapis.com/api/error_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("storage.googleapis.com/api/latency", unit="Milliseconds", aggregation="Mean", interval="PT5M"),
            
            # Object operations
            MetricSpec("storage.googleapis.com/storage/object_count", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("storage.googleapis.com/storage/object_size_distribution", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Cost-related metrics
            MetricSpec("storage.googleapis.com/storage/class_a_request_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("storage.googleapis.com/storage/class_b_request_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("storage.googleapis.com/storage/download_bytes", unit="Bytes", aggregation="Sum", interval="PT5M"),
            MetricSpec("storage.googleapis.com/storage/upload_bytes", unit="Bytes", aggregation="Sum", interval="PT5M"),
            
            # Delete operations
            MetricSpec("storage.googleapis.com/storage/delete_count", unit="Count", aggregation="Sum", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=CLOUD_STORAGE_METRICS_TABLE,
    ),
)
