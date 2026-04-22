"""GCP Cloud Load Balancer service definition — pure config, no logic.

Declares how to fetch Google Cloud Load Balancing (Forwarding Rules and Backend Services)
and which Iceberg tables to write them to.
"""

from google.cloud import compute_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

LB_FORWARDING_RULES_TABLE = TableConfig(
    table_name="bronze_gcp_loadbalancer_forwarding_rules",
    s3_path_suffix="gcp/lb_forwarding_rules",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "region": "string",
        "network": "string",
        "load_balancing_scheme": "string",
        "ip_address": "string",
        "ip_protocol": "string",
        "port_range": "string",
        "backend_service": "string",
        "description": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

LB_BACKEND_SERVICES_TABLE = TableConfig(
    table_name="bronze_gcp_loadbalancer_backend_services",
    s3_path_suffix="gcp/lb_backend_services",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "protocol": "string",
        "port": "int",
        "health_checks": "string",
        "session_affinity": "string",
        "load_balancing_scheme": "string",
        "description": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

LB_METRICS_TABLE = TableConfig(
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

FORWARDING_RULE_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "resource_name",
    "region": "region",
    "network": "network",
    "load_balancing_scheme": "load_balancing_scheme",
    "I_p_address": "ip_address",
    "I_p_protocol": "ip_protocol",
    "port_range": "port_range",
    "backend_service": "backend_service",
    "description": "description",
}

BACKEND_SERVICE_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "resource_name",
    "protocol": "protocol",
    "port": "port",
    "health_checks": "health_checks", # Typically a list, SDK might return list of strings
    "session_affinity": "session_affinity",
    "load_balancing_scheme": "load_balancing_scheme",
    "description": "description",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_LOAD_BALANCER_SERVICE = ServiceDefinition(
    name="CloudLoadBalancer",
    namespace="loadbalancing.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=compute_v1.ForwardingRulesClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=FORWARDING_RULE_FIELD_MAPPING,
            table_config=LB_FORWARDING_RULES_TABLE,
        ),
        ResourceFetcher(
            sdk_client_class=compute_v1.BackendServicesClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=BACKEND_SERVICE_FIELD_MAPPING,
            table_config=LB_BACKEND_SERVICES_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            MetricSpec("loadbalancing.googleapis.com/https/request_count", unit="Count", aggregation="Average", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/backend_latencies", unit="Milliseconds", aggregation="Average", interval="PT5M"),
            MetricSpec("loadbalancing.googleapis.com/https/response_bytes_count", unit="Bytes", aggregation="Average", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=LB_METRICS_TABLE,
    ),
)
