"""GCP Cloud Scheduler service definition - pure config, no logic.

Declares how to fetch Cloud Scheduler jobs from GCP,
and which Iceberg tables to write them to.

Notes:
- Cloud Scheduler manages cron-like scheduled jobs
- resource_id is composed as "project_id.job_name" for unique identification
- Metrics are collected from scheduler.googleapis.com namespace
- Idle signal: no job executions over 14-30 days
- Overprovisioned signal: high execution frequency with poor success rates
"""

from google.cloud import scheduler_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUD_SCHEDULER_TABLE = TableConfig(
    table_name="bronze_gcp_cloud_scheduler",
    s3_path_suffix="gcp/cloud_scheduler",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.job_name
        "resource_name": "string",          # Job display name
        "project_id": "string",
        "job_name": "string",
        "description": "string",
        "schedule": "string",               # Cron schedule expression
        "time_zone": "string",              # Time zone for schedule
        "state": "string",                  # ENABLED, PAUSED, DISABLED
        "attempt_deadline": "string",        # Deadline for job attempts
        "retry_count": "int",               # Number of retry attempts
        "min_backoff_duration": "string",   # Minimum backoff between retries
        "max_backoff_duration": "string",   # Maximum backoff between retries
        "max_doublings": "int",             # Maximum backoff doublings
        "http_target_url": "string",        # HTTP target URL
        "http_target_http_method": "string", # HTTP method
        "http_target_body": "string",       # HTTP request body
        "pubsub_target_topic_name": "string", # Pub/Sub topic target
        "pubsub_target_data": "string",     # Pub/Sub message data
        "app_engine_target_service": "string", # App Engine service
        "app_engine_target_version": "string", # App Engine version
        "app_engine_target_host": "string", # App Engine host
        "app_engine_target_relative_uri": "string", # App Engine URI
        "oidc_token_service_account_email": "string", # OIDC service account
        "oidc_token_audience": "string",   # OIDC audience
        "oauth_token_service_account_email": "string", # OAuth service account
        "oauth_token_scope": "string",      # OAuth scope
        "labels": "map<string,string>",
        "create_time": "string",
        "update_time": "string",
        "last_attempt_time": "string",     # Last execution attempt
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUD_SCHEDULER_METRICS_TABLE = TableConfig(
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

CLOUD_SCHEDULER_FIELD_MAPPING = {
    "name": "job_name",
    "description": "description",
    "schedule": "schedule",
    "time_zone": "time_zone",
    "state": "state",
    "retry_config.retry_count": "retry_count",
    "retry_config.min_backoff_duration": "min_backoff_duration",
    "retry_config.max_backoff_duration": "max_backoff_duration",
    "retry_config.max_doublings": "max_doublings",
    "attempt_deadline": "attempt_deadline",
    "http_target.uri": "http_target_url",
    "http_target.http_method": "http_target_http_method",
    "http_target.body": "http_target_body",
    "pubsub_target.topic_name": "pubsub_target_topic_name",
    "pubsub_target.data": "pubsub_target_data",
    "app_engine_target.service": "app_engine_target_service",
    "app_engine_target.version": "app_engine_target_version",
    "app_engine_target.host": "app_engine_target_host",
    "app_engine_target.relative_uri": "app_engine_target_relative_uri",
    "oidc_token.service_account_email": "oidc_token_service_account_email",
    "oidc_token.audience": "oidc_token_audience",
    "oauth_token.service_account_email": "oauth_token_service_account_email",
    "oauth_token.scope": "oauth_token_scope",
    "labels": "labels",
    "create_time": "create_time",
    "update_time": "update_time",
    "last_attempt_time": "last_attempt_time",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUD_SCHEDULER_SERVICE = ServiceDefinition(
    name="Cloud Scheduler",
    namespace="scheduler.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=scheduler_v1.CloudSchedulerClient,
            list_method="list_jobs",
            field_mapping=CLOUD_SCHEDULER_FIELD_MAPPING,
            table_config=CLOUD_SCHEDULER_TABLE,
            composite_id_fields=("project_id", "job_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Execution metrics
            MetricSpec("scheduler.googleapis.com/job/attempt_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/attempt_success_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/attempt_failure_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/latency", unit="Milliseconds", aggregation="Mean", interval="PT5M"),
            
            # Error metrics
            MetricSpec("scheduler.googleapis.com/job/error_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/timeout_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/permission_denied_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # HTTP-specific metrics
            MetricSpec("scheduler.googleapis.com/job/http_2xx_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/http_4xx_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/http_5xx_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Pub/Sub-specific metrics
            MetricSpec("scheduler.googleapis.com/job/pubsub_success_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/pubsub_failure_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # App Engine-specific metrics
            MetricSpec("scheduler.googleapis.com/job/appengine_success_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/appengine_failure_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Retry metrics
            MetricSpec("scheduler.googleapis.com/job/retry_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/retry_success_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/retry_failure_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Active job metrics
            MetricSpec("scheduler.googleapis.com/job/active_count", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/paused_count", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("scheduler.googleapis.com/job/disabled_count", unit="Count", aggregation="Gauge", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=CLOUD_SCHEDULER_METRICS_TABLE,
    ),
)
