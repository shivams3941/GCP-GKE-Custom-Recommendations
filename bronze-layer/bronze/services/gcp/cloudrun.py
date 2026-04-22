"""GCP Cloud Run service definition — pure config, no logic.

Declares how to fetch Cloud Run services, revisions, and metrics from GCP,
and which Iceberg tables to write them to.
"""

from google.cloud import run_v2

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUDRUN_SERVICES_TABLE = TableConfig(
    table_name="bronze_gcp_cloudrun_services",
    s3_path_suffix="gcp/cloudrun_services",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "region": "string",
        "description": "string",
        "ingress": "string",
        "launch_stage": "string",
        "vcpu_limit": "string",
        "memory_limit": "string",
        "cpu_allocation": "string",
        "concurrency_max": "int",
        "min_instances": "int",
        "max_instances": "int",
        "timeout_seconds": "int",
        "service_account": "string",
        "labels": "string",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDRUN_REVISIONS_TABLE = TableConfig(
    table_name="bronze_gcp_cloudrun_revisions",
    s3_path_suffix="gcp/cloudrun_revisions",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "revision_name": "string",
        "service_name": "string",
        "region": "string",
        "vcpu_limit": "string",
        "memory_limit": "string",
        "concurrency_max": "int",
        "timeout_seconds": "int",
        "execution_environment": "string",
        "labels": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDRUN_METRICS_TABLE = TableConfig(
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

CLOUDRUN_SERVICE_FIELD_MAPPING = {
    "name": "resource_id",
    # resource_name derived from "name" — same source as resource_id
    # use framework alias or derive at transform time to avoid duplicate key
    "description": "description",
    "ingress": "ingress",
    "launch_stage": "launch_stage",
    "template.containers.resources.limits.cpu": "vcpu_limit",
    "template.containers.resources.limits.memory": "memory_limit",
    "template.scaling.max_instance_count": "max_instances",
    "template.scaling.min_instance_count": "min_instances",
    "template.max_instance_request_concurrency": "concurrency_max",
    "template.timeout": "timeout_seconds",
    "template.service_account": "service_account",
    "labels": "labels",
}

CLOUDRUN_REVISION_FIELD_MAPPING = {
    "name": "resource_id",
    # revision_name derived from "name" — same source as resource_id
    # derive at transform time to avoid duplicate key
    "service": "service_name",
    "scaling.max_instance_count": "max_instances",
    "scaling.min_instance_count": "min_instances",
    "max_instance_request_concurrency": "concurrency_max",
    "timeout": "timeout_seconds",
    "execution_environment": "execution_environment",
    "labels": "labels",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUDRUN_SERVICE = ServiceDefinition(
    name="CloudRun",
    namespace="run.googleapis.com/Service",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=run_v2.ServicesClient,
            list_method="list_services",
            field_mapping=CLOUDRUN_SERVICE_FIELD_MAPPING,
            table_config=CLOUDRUN_SERVICES_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Idle detection metrics
            MetricSpec(
                "run.googleapis.com/request_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/instance_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/cpu/utilizations",
                unit="Percent",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/memory/utilizations",
                unit="Percent",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/network/received_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/network/sent_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            # Overprovisioned detection metrics
            MetricSpec(
                "run.googleapis.com/container/cpu/utilizations",
                unit="Percent",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/memory/utilizations",
                unit="Percent",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/request_concurrency",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/request_concurrency",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/container/instance_count",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/request_latencies",
                unit="Milliseconds",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "run.googleapis.com/request_latencies",
                unit="Milliseconds",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
        ],
        resource_id_field="resource_id",
        table_config=CLOUDRUN_METRICS_TABLE,
    ),
)
