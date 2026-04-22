"""GCP Cloud Build service definition — pure config, no logic.

Declares how to fetch Cloud Build triggers, builds, and metrics from GCP,
and which Iceberg tables to write them to.
"""

from google.cloud.devtools import cloudbuild_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUDBUILD_TRIGGERS_TABLE = TableConfig(
    table_name="bronze_gcp_cloudbuild_triggers",
    s3_path_suffix="gcp/cloudbuild_triggers",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "description": "string",
        "trigger_type": "string",
        "disabled": "boolean",
        "filename": "string",
        "repo_type": "string",
        "repo_name": "string",
        "branch_name": "string",
        "tag_name": "string",
        "substitutions": "string",
        "included_files": "string",
        "ignored_files": "string",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDBUILD_BUILDS_TABLE = TableConfig(
    table_name="bronze_gcp_cloudbuild_builds",
    s3_path_suffix="gcp/cloudbuild_builds",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "build_id": "string",
        "project_id": "string",
        "status": "string",
        "create_time": "string",
        "start_time": "string",
        "finish_time": "string",
        "timeout": "string",
        "images": "string",
        "queue_ttl": "string",
        "logs_bucket": "string",
        "source_type": "string",
        "steps_count": "int",
        "substitutions": "string",
        "tags": "string",
        "service_account": "string",
        "machine_type": "string",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDBUILD_METRICS_TABLE = TableConfig(
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

CLOUDBUILD_TRIGGER_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "resource_name",
    "description": "description",
    "disabled": "disabled",
    "filename": "filename",
    "trigger_template.repo_type": "repo_type",
    "trigger_template.repo_name": "repo_name",
    "trigger_template.branch_name": "branch_name",
    "trigger_template.tag_name": "tag_name",
    "substitutions": "substitutions",
    "included_files": "included_files",
    "ignored_files": "ignored_files",
}

CLOUDBUILD_BUILD_FIELD_MAPPING = {
    "id": "resource_id",
    # build_id is the same as resource_id — derive at transform time to avoid duplicate key
    "project_id": "project_id",
    "status": "status",
    "create_time": "create_time",
    "start_time": "start_time",
    "finish_time": "finish_time",
    "timeout": "timeout",
    "images": "images",
    "queue_ttl": "queue_ttl",
    "logs_bucket": "logs_bucket",
    "source.storage_source": "source_type",
    "steps": "steps_count",
    "substitutions": "substitutions",
    "tags": "tags",
    "service_account": "service_account",
    "options.machine_type": "machine_type",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUDBUILD_SERVICE = ServiceDefinition(
    name="CloudBuild",
    namespace="cloudbuild.googleapis.com/Build",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=cloudbuild_v1.CloudBuildClient,
            list_method="list_build_triggers",
            field_mapping=CLOUDBUILD_TRIGGER_FIELD_MAPPING,
            table_config=CLOUDBUILD_TRIGGERS_TABLE,
        ),
        ResourceFetcher(
            sdk_client_class=cloudbuild_v1.CloudBuildClient,
            list_method="list_builds",
            field_mapping=CLOUDBUILD_BUILD_FIELD_MAPPING,
            table_config=CLOUDBUILD_BUILDS_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Idle detection metrics
            MetricSpec(
                "cloudbuild.googleapis.com/build/count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/trigger_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/duration",
                unit="Seconds",
                aggregation="Average",
                interval="PT5M",
            ),
            # Overprovisioned detection metrics
            MetricSpec(
                "cloudbuild.googleapis.com/build/duration",
                unit="Seconds",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/duration",
                unit="Seconds",
                aggregation="Maximum",
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/count",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/concurrent_builds",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/concurrent_builds",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/queue_time",
                unit="Seconds",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "cloudbuild.googleapis.com/build/status",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
        ],
        resource_id_field="resource_id",
        table_config=CLOUDBUILD_METRICS_TABLE,
    ),
)
