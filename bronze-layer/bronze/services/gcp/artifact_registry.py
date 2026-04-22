"""GCP Artifact Registry service definition — pure config, no logic.

Declares how to fetch Artifact Registry repositories from GCP,
and which Iceberg tables to write them to.

Authentication:
- GCP credentials are loaded at runtime via bronze.auth.gcp_auth.get_gcp_credentials()
- The service account key JSON is stored in AWS Secrets Manager under
  the secret name defined in GCP_SECRET_NAME (gcp/devops-internal/service-account)

Notes:
- resource_id is composed as "project_id.location.repository_name" for a clean,
  unambiguous primary key across multi-project/multi-region setups.
- Metrics are project-level under the artifactregistry.googleapis.com namespace.
- Idle signal: request_count + pull_count + push_count near-zero over 14–30 days.
- Overprovisioned signal: high stored_bytes with very low request_count over 30 days.
- Cleanup policies are the primary cost lever — always check before recommending deletion.
- Multi-region repositories cannot be downgraded to regional; migration requires
  creating a new regional repository and re-pushing artifacts.
"""

from google.cloud import artifactregistry_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

ARTIFACT_REGISTRY_REPOSITORIES_TABLE = TableConfig(
    table_name="bronze_gcp_artifact_registry_repositories",
    s3_path_suffix="gcp/artifact_registry_repositories",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.location.repository_name
        "resource_name": "string",          # repository display name
        "project_id": "string",
        "location": "string",               # region (e.g. us-central1) or multi-region (e.g. us)
        "repository_name": "string",
        "description": "string",
        "format": "string",                 # DOCKER, MAVEN, NPM, PYTHON, HELM, APT, YUM, GENERIC
        "mode": "string",                   # STANDARD_REPOSITORY, VIRTUAL_REPOSITORY, REMOTE_REPOSITORY
        "kms_key_name": "string",           # CMEK key if set
        "size_bytes": "double",             # total stored bytes
        "cleanup_policy_dry_run": "string", # "true" / "false"
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

ARTIFACT_REGISTRY_METRICS_TABLE = TableConfig(
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

ARTIFACT_REGISTRY_REPOSITORY_FIELD_MAPPING = {
    "name": "repository_name",          # full resource name; trimmed to short name at transform time
    "description": "description",
    "format_": "format",                # SDK uses format_ to avoid Python keyword clash
    "mode": "mode",
    "kms_key_name": "kms_key_name",
    "size_bytes": "size_bytes",
    "cleanup_policies": "cleanup_policy_dry_run",  # presence indicates cleanup policy configured
    "labels": "labels",
    "create_time": "create_time",
    "update_time": "update_time",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

ARTIFACT_REGISTRY_SERVICE = ServiceDefinition(
    name="ARTIFACT_REGISTRY",
    namespace="artifactregistry.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=artifactregistry_v1.ArtifactRegistryClient,
            list_method="list_repositories",
            field_mapping=ARTIFACT_REGISTRY_REPOSITORY_FIELD_MAPPING,
            table_config=ARTIFACT_REGISTRY_REPOSITORIES_TABLE,
            composite_id_fields=("project_id", "location", "repository_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # --- Idle detection ---
            MetricSpec(
                "artifactregistry.googleapis.com/repository/request_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "artifactregistry.googleapis.com/repository/api/request_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Storage / overprovisioned ---
            MetricSpec(
                "artifactregistry.googleapis.com/repository/size",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Remote repository quota utilisation ---
            MetricSpec(
                "artifactregistry.googleapis.com/quota/project_region_upstream_host_reads/usage",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "artifactregistry.googleapis.com/quota/project_region_upstream_host_reads/limit",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Data plane request latency (avg ms per repo) ---
            # High latency with low request_count may indicate cold/stale repo
            MetricSpec(
                "artifactregistry.googleapis.com/repository/request_latencies",
                unit="us",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Control plane request latency (avg ms per repo) ---
            # Tracks latency for repo management operations (create, update, delete)
            MetricSpec(
                "artifactregistry.googleapis.com/repository/api/request_latencies",
                unit="us",
                aggregation="Average",
                interval="PT5M",
            ),
            # --- Control plane request count per repo ---
            # Tracks management API calls (not data pulls); useful for activity baseline
            MetricSpec(
                "artifactregistry.googleapis.com/repository/api/request_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
        ],
        resource_id_field="resource_id",
        table_config=ARTIFACT_REGISTRY_METRICS_TABLE,
    ),
)
