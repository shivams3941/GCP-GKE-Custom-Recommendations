"""GCP Cloud Functions service definition — pure config, no logic.

Declares how to fetch Cloud Functions (1st gen and 2nd gen) from GCP,
and which Iceberg tables to write them to.

Authentication:
- GCP credentials are loaded at runtime via bronze.auth.gcp_auth.get_gcp_credentials()
- The service account key JSON is stored in AWS Secrets Manager under
  the secret name defined in GCP_SECRET_NAME (gcp/devops-internal/service-account)

Notes:
- resource_id is composed as "project_id.location.function_name" for a clean,
  unambiguous primary key across multi-project/multi-region setups.
- 2nd gen functions are built on Cloud Run; CPU metrics come from the
  run.googleapis.com namespace in addition to cloudfunctions.googleapis.com.
- Idle signal: execution_count + active_instances near-zero over 14–30 days.
- Overprovisioned signals:
    - user_memory_bytes P95 < 40% of provisioned memory
    - execution_times P95 < 30% of configured timeout
    - active_instances max < 30% of max_instances setting
    - min_instances > 0 with very low invocation rate (idle billing)
- 1st gen: memory tier is the only sizing lever (CPU scales implicitly).
- 2nd gen: memory and CPU are independent levers; prefer for new functions.
- Always prefer the latest supported runtime (e.g. python312 over python39).
- min_instances > 0 incurs idle charges even with zero traffic — always flag.
"""

from google.cloud import functions_v2

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUD_FUNCTIONS_TABLE = TableConfig(
    table_name="bronze_gcp_cloud_functions",
    s3_path_suffix="gcp/cloud_functions",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.location.function_name
        "resource_name": "string",          # function display name
        "project_id": "string",
        "location": "string",               # region (e.g. us-central1)
        "function_name": "string",
        "description": "string",
        "state": "string",                  # ACTIVE, FAILED, DEPLOYING, DELETING, UNKNOWN
        "environment": "string",            # GEN_1 or GEN_2
        "runtime": "string",                # e.g. python312, nodejs20
        "entry_point": "string",
        "available_memory": "string",       # e.g. "256M", "1Gi"
        "available_cpu": "string",          # 2nd gen only; e.g. "1", "0.5"
        "timeout_seconds": "double",        # max execution duration
        "min_instance_count": "double",     
        "max_instance_count": "double",     # upper scaling limit
        "max_instance_request_concurrency": "double",  # 2nd gen only
        "ingress_settings": "string",       # ALLOW_ALL, ALLOW_INTERNAL_ONLY, etc.
        "vpc_connector": "string",
        "service_account_email": "string",
        "uri": "string",                    
        "build_id": "string",
        "docker_repository": "string",      # Artifact Registry repo for the image
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

CLOUD_FUNCTIONS_METRICS_TABLE = TableConfig(
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

CLOUD_FUNCTION_FIELD_MAPPING = {
    "name": "function_name",            # full resource name; trimmed to short name at transform time
    "description": "description",
    "state": "state",
    "environment": "environment",       # GEN_1 or GEN_2
    # Build config
    "build_config.runtime": "runtime",
    "build_config.entry_point": "entry_point",
    "build_config.docker_repository": "docker_repository",
    "build_config.build": "build_id",
    # Service config (2nd gen) / resource config (1st gen)
    "service_config.available_memory": "available_memory",
    "service_config.available_cpu": "available_cpu",
    "service_config.timeout_seconds": "timeout_seconds",
    "service_config.min_instance_count": "min_instance_count",
    "service_config.max_instance_count": "max_instance_count",
    "service_config.max_instance_request_concurrency": "max_instance_request_concurrency",
    "service_config.ingress_settings": "ingress_settings",
    "service_config.vpc_connector": "vpc_connector",
    "service_config.service_account_email": "service_account_email",
    "service_config.uri": "uri",
    # Metadata
    "labels": "labels",
    "create_time": "create_time",
    "update_time": "update_time",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_FUNCTIONS_SERVICE = ServiceDefinition(
    name="CLOUD_FUNCTIONS",
    namespace="cloudfunctions.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=functions_v2.FunctionServiceClient,
            list_method="list_functions",
            field_mapping=CLOUD_FUNCTION_FIELD_MAPPING,
            table_config=CLOUD_FUNCTIONS_TABLE,
            composite_id_fields=("project_id", "location", "function_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # --- Idle detection ---
            MetricSpec(
                "cloudfunctions.googleapis.com/function/execution_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "cloudfunctions.googleapis.com/function/active_instances",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Memory utilisation ---
            MetricSpec(
                "cloudfunctions.googleapis.com/function/user_memory_bytes",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Execution duration ---
            MetricSpec(
                "cloudfunctions.googleapis.com/function/execution_times",
                unit="ns",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Instance count ---
            MetricSpec(
                "cloudfunctions.googleapis.com/function/instance_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Network egress bytes (data sent out per function) ---
            # High egress with low execution_count = expensive idle traffic
            MetricSpec(
                "cloudfunctions.googleapis.com/function/network_egress",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Missed executions (quota / concurrency limit hits) ---
            # Non-zero value means invocations were dropped
            MetricSpec(
                "cloudfunctions.googleapis.com/function/missed_execution_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Execution time per invocation (2nd gen / Cloud Run) ---
            # Tracks per-request CPU time; complements execution_times
            MetricSpec(
                "cloudfunctions.googleapis.com/function/execution_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
        ],
        resource_id_field="resource_id",
        table_config=CLOUD_FUNCTIONS_METRICS_TABLE,
    ),
)
