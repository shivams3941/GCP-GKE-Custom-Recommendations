"""GCP Cloud CDN service definition - pure config, no logic.

Declares how to fetch Cloud CDN configurations and metrics from GCP,
and which Iceberg tables to write them to.

Notes:
- Cloud CDN is part of Cloud Load Balancing - data comes from compute_v1 APIs
- resource_id is composed as "project_id.url_map_name" for unique identification
- Metrics are collected from loadbalancing.googleapis.com namespace
- Idle signal: no request traffic over 14-30 days
- Overprovisioned signal: high cache miss ratios or low utilization
"""

from google.cloud import compute_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUD_CDN_TABLE = TableConfig(
    table_name="bronze_gcp_cloud_cdn",
    s3_path_suffix="gcp/cloud_cdn",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.url_map_name
        "resource_name": "string",          # URL map display name
        "project_id": "string",
        "url_map_name": "string",
        "description": "string",
        "default_service": "string",       # backend service name
        "cdn_enabled": "boolean",           # CDN enabled flag
        "cache_mode": "string",             # CACHE_STATIC, CACHE_ALL, etc.
        "client_ttl": "int",               # Client cache TTL
        "max_ttl": "int",                  # Maximum cache TTL
        "default_ttl": "int",              # Default cache TTL
        "negative_caching": "boolean",     # Negative caching enabled
        "serve_while_stale": "boolean",    # Serve while stale enabled
        "bypass_cache_on_request_headers": "string",  # Bypass cache headers
        "compressed": "boolean",            # Compression enabled
        "region": "string",
        "labels": "map<string,string>",
        "create_time": "string",
        "update_time": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUD_CDN_METRICS_TABLE = TableConfig(
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

CLOUD_CDN_FIELD_MAPPING = {
    "name": "url_map_name",
    "description": "description",
    "default_service": "default_service",
    "region": "region",
    "resource_labels": "labels",
    "creation_timestamp": "create_time",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_CDN_SERVICE = ServiceDefinition(
    name="Cloud CDN",
    namespace="loadbalancing.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=compute_v1.UrlMapsClient,
            list_method="list",
            field_mapping=CLOUD_CDN_FIELD_MAPPING,
            table_config=CLOUD_CDN_TABLE,
            composite_id_fields=("project_id", "url_map_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Request metrics
            MetricSpec("loadbalancing.googleapis.com/https/request_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/backend_request_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/response_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Cache metrics
            MetricSpec("loadbalancing.googleapis.com/https/cache_hit_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/cache_miss_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/cache_fill_bytes", unit="Bytes", aggregation="Sum", interval="PT5M"),
            
            # Latency metrics
            MetricSpec("loadbalancing.googleapis.com/https/total_latencies", unit="Milliseconds", aggregation="Mean", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/backend_latencies", unit="Milliseconds", aggregation="Mean", interval="PT5M"),
            
            # Bandwidth metrics
            MetricSpec("loadbalancing.googleapis.com/https/sent_bytes_count", unit="Bytes", aggregation="Sum", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/received_bytes_count", unit="Bytes", aggregation="Sum", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=CLOUD_CDN_METRICS_TABLE,
    ),
)
