"""GCP Cloud Armor service definition — pure config, no logic.

Declares how to fetch Google Cloud Armor Security Policies and metrics,
and which Iceberg tables to write them to.
"""

from google.cloud import compute_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

SECURITY_POLICIES_TABLE = TableConfig(
    table_name="bronze_gcp_security_policies",
    s3_path_suffix="gcp/security_policies",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "description": "string",
        "type": "string",
        "fingerprint": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

ARMOR_METRICS_TABLE = TableConfig(
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

SECURITY_POLICY_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "resource_name",
    "description": "description",
    "type_": "type",
    "fingerprint": "fingerprint",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_ARMOR_SERVICE = ServiceDefinition(
    name="CloudArmor",
    namespace="networksecurity.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=compute_v1.SecurityPoliciesClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=SECURITY_POLICY_FIELD_MAPPING,
            table_config=SECURITY_POLICIES_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            MetricSpec("networksecurity.googleapis.com/external_waf/request_count", unit="Count", aggregation="Average", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=ARMOR_METRICS_TABLE,
    ),
)
