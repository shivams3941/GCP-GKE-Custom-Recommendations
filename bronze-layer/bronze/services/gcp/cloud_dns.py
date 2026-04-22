"""GCP Cloud DNS service definition - pure config, no logic.

Declares how to fetch Cloud DNS configurations and metrics from GCP,
and which Iceberg tables to write them to.

Notes:
- Cloud DNS manages DNS zones and record sets
- resource_id is composed as "project_id.zone_name" for unique identification
- Metrics are collected from dns.googleapis.com namespace
- Idle signal: no DNS queries over 14-30 days
- Overprovisioned signal: high query volume with poor performance
"""

from google.cloud import dns_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUD_DNS_TABLE = TableConfig(
    table_name="bronze_gcp_cloud_dns",
    s3_path_suffix="gcp/cloud_dns",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.zone_name
        "resource_name": "string",          # DNS zone display name
        "project_id": "string",
        "zone_name": "string",
        "description": "string",
        "dns_name": "string",               # DNS zone name (e.g., example.com.)
        "visibility": "string",             # PUBLIC, PRIVATE
        "creation_time": "string",
        "labels": "map<string,string>",
        "name_server_set": "string",       # Name server set for private zones
        "peering_config": "string",         # DNS peering configuration
        "forwarding_config": "string",      # DNS forwarding configuration
        "service_directory_config": "string", # Service Directory config
        "record_set_count": "int",          # Number of record sets
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUD_DNS_METRICS_TABLE = TableConfig(
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

CLOUD_DNS_FIELD_MAPPING = {
    "name": "zone_name",
    "description": "description",
    "dns_name": "dns_name",
    "visibility": "visibility",
    "creation_time": "creation_time",
    "labels": "labels",
    "name_server_set": "name_server_set",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_DNS_SERVICE = ServiceDefinition(
    name="Cloud DNS",
    namespace="dns.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=dns_v1.ManagedZonesClient,
            list_method="list_managed_zones",
            field_mapping=CLOUD_DNS_FIELD_MAPPING,
            table_config=CLOUD_DNS_TABLE,
            composite_id_fields=("project_id", "zone_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Query metrics
            MetricSpec("dns.googleapis.com/query_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("dns.googleapis.com/response_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("dns.googleapis.com/success_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("dns.googleapis.com/rcode_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Latency metrics
            MetricSpec("dns.googleapis.com/latency", unit="Milliseconds", aggregation="Mean", interval="PT5M"),
            
            # Error metrics
            MetricSpec("dns.googleapis.com/nxdomain_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("dns.googleapis.com/servfail_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("dns.googleapis.com/formerr_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Resource metrics
            MetricSpec("dns.googleapis.com/record_set_count", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("dns.googleapis.com/zone_count", unit="Count", aggregation="Gauge", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=CLOUD_DNS_METRICS_TABLE,
    ),
)
