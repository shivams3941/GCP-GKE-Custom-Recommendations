"""GCP Dataflow service definition — pure config, no logic.

Declares how to fetch Dataflow jobs from GCP and which Iceberg tables to write them to.

Authentication:
- GCP credentials are loaded at runtime via bronze.auth.gcp_auth.get_gcp_credentials()
- The service account key JSON is stored in AWS Secrets Manager under
  the secret name defined in GCP_SECRET_NAME (gcp/devops-internal/service-account)

Notes:
- Dataflow jobs are project+region scoped; we use aggregated listing across all
  known regions via the REST-based googleapiclient (the Dataflow SDK does not
  expose a cross-region list method).
- resource_id is composed as "project_id.region.job_id" for a clean, unambiguous
  primary key across multi-project/multi-region setups.
- Metrics are emitted at the job level under the dataflow_job monitored resource
  type in Cloud Monitoring with an ~60-second lag.
- Idle signals: element_count near zero, system_lag = 0, watermark not advancing.
- Overprovisioned signals: cpu_utilization P95 < 25%, current_num_vcpus at ceiling
  with low lag, estimated_bytes P95 < 20% of disk allocation, memory_usage P95 < 30%.

Metrics covered:
  Idle:          dataflow.googleapis.com/job/element_count
                 dataflow.googleapis.com/job/system_lag
                 dataflow.googleapis.com/job/data_watermark_lag
                 dataflow.googleapis.com/job/current_num_vcpus (idle proxy)
  Overprovisioned: dataflow.googleapis.com/job/current_num_vcpus
                   dataflow.googleapis.com/job/cpu_utilization
                   dataflow.googleapis.com/job/estimated_bytes
                   dataflow.googleapis.com/job/system_lag (low lag + high workers)
                   dataflow.googleapis.com/job/memory_usage
"""

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

DATAFLOW_JOBS_TABLE = TableConfig(
    table_name="bronze_gcp_dataflow_jobs",
    s3_path_suffix="gcp/dataflow_jobs",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.region.job_id
        "resource_name": "string",          # job display name
        "project_id": "string",
        "region": "string",
        "job_id": "string",
        "job_name": "string",
        "job_type": "string",               # JOB_TYPE_BATCH or JOB_TYPE_STREAMING
        "current_state": "string",          # JOB_STATE_RUNNING, JOB_STATE_DONE, etc.
        "current_state_time": "string",
        "runner_config": "string",          # JSON: pipeline options snapshot
        "sdk_version": "string",
        "environment_worker_zone": "string",
        "environment_machine_type": "string",
        "environment_max_workers": "double",
        "environment_num_workers": "double",
        "environment_disk_size_gb": "double",
        "environment_network": "string",
        "environment_subnetwork": "string",
        "environment_service_account": "string",
        "environment_temp_location": "string",
        "environment_experiments": "string",    # comma-separated experiment flags
        "create_time": "string",
        "start_time": "string",
        "labels": "map<string,string>",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

DATAFLOW_METRICS_TABLE = TableConfig(
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
# Field mappings: REST API response key → output column name
# (Dataflow REST API returns plain dicts, not protobuf objects)
# Note: This mapping is used by the framework's generic field extractor.
# For complex nested structures (workerPools array, experiments), custom
# extraction logic in the runner is required.
# ---------------------------------------------------------------------------

DATAFLOW_JOB_FIELD_MAPPING = {
    "id": "job_id",
    "name": "job_name",
    "type": "job_type",
    "currentState": "current_state",
    "currentStateTime": "current_state_time",
    "createTime": "create_time",
    "startTime": "start_time",
    "labels": "labels",
}

# ---------------------------------------------------------------------------
# Service definition
# Note: Dataflow uses the REST API (googleapiclient) rather than a typed SDK
# client, so resource_fetchers is intentionally empty — the local runner and
# GCPServiceRunner subclass handle listing directly via the REST client.
# Metrics are fetched via Cloud Monitoring as normal.
# ---------------------------------------------------------------------------

DATAFLOW_SERVICE = ServiceDefinition(
    name="Dataflow",
    namespace="dataflow.googleapis.com",
    resource_fetchers=[],   # REST-based; see DataflowServiceRunner in local test
    metrics=MetricDefinition(
        metric_specs=[
            # --- Idle detection ---
            # Elements processed near zero — job running but no data flowing
            MetricSpec(
                "dataflow.googleapis.com/job/element_count",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # Streaming: system lag = 0 sustained — source has no backlog
            MetricSpec(
                "dataflow.googleapis.com/job/system_lag",
                unit="Seconds",
                aggregation="Average",
                interval="PT1H",
            ),
            # Streaming: watermark not advancing — no new events arriving
            # Note: only emitted by streaming jobs; batch jobs will return no data
            MetricSpec(
                "dataflow.googleapis.com/job/data_watermark_lag",
                unit="Seconds",
                aggregation="Average",
                interval="PT1H",
            ),
            # Workers provisioned but utilisation near zero
            MetricSpec(
                "dataflow.googleapis.com/job/current_num_vcpus",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # --- Overprovisioned detection ---
            # CPU utilisation P95 < 25% per worker over 14 days
            MetricSpec(
                "dataflow.googleapis.com/job/cpu_utilization",
                unit="Ratio",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
            MetricSpec(
                "dataflow.googleapis.com/job/cpu_utilization",
                unit="Ratio",
                aggregation="Average",
                interval="PT1H",
            ),
            # vCPU count P95 — ceiling never reached signals over-provisioned maxNumWorkers
            MetricSpec(
                "dataflow.googleapis.com/job/current_num_vcpus",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
            # Shuffle bytes P95 < 20% of disk allocation — enable Dataflow Shuffle
            MetricSpec(
                "dataflow.googleapis.com/job/estimated_bytes",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
            # Streaming lag near zero with high worker count — over-provisioned
            MetricSpec(
                "dataflow.googleapis.com/job/system_lag",
                unit="Seconds",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
            # Memory usage P95 < 30% of allocated memory per worker
            MetricSpec(
                "dataflow.googleapis.com/job/memory_usage",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
        ],
        resource_id_field="resource_id",
        table_config=DATAFLOW_METRICS_TABLE,
    ),
)
