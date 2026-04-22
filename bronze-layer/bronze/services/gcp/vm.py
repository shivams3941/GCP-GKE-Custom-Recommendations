"""GCP Compute Engine service definition — pure config, no logic.

Declares how to fetch Compute Engine instances, disks, and VM metrics from GCP,
and which Iceberg tables to write them to.
"""

from google.cloud import compute_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

VM_RESOURCES_TABLE = TableConfig(
    table_name="bronze_gcp_compute_instances",
    s3_path_suffix="gcp/compute_instances",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "zone": "string",
        "region": "string",
        "machine_type": "string",
        "machine_family": "string",
        "cpu_platform": "string",
        "status": "string",
        "preemptible": "boolean",
        "spot": "boolean",
        "disk_count": "int",
        "network_interface_count": "int",
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

VM_DISKS_TABLE = TableConfig(
    table_name="bronze_gcp_disks",
    s3_path_suffix="gcp/disks",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "name": "string",
        "disk_size_gb": "int",
        "disk_type": "string",
        "status": "string",
        "zone": "string",
        "region": "string",
        "attached_to": "string",
        "provisioned_iops": "int",
        "provisioned_throughput": "int",
        "labels": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

VM_METRICS_TABLE = TableConfig(
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
        "zone": "string",
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

VM_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "resource_name",
    "zone": "zone",
    "machine_type": "machine_type",
    "cpu_platform": "cpu_platform",
    "status": "status",
    "scheduling.preemptible": "preemptible",
    "scheduling.provisioning_model": "spot",
    "labels": "labels",
}

DISK_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "name",
    "size_gb": "disk_size_gb",
    "type": "disk_type",
    "status": "status",
    "zone": "zone",
    "users": "attached_to",
    "provisioned_iops": "provisioned_iops",
    "provisioned_throughput": "provisioned_throughput",
    "labels": "labels",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

COMPUTE_ENGINE_SERVICE = ServiceDefinition(
    name="ComputeEngine",
    namespace="compute.googleapis.com/Instance",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=compute_v1.InstancesClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=VM_FIELD_MAPPING,
            table_config=VM_RESOURCES_TABLE,
        ),
        ResourceFetcher(
            sdk_client_class=compute_v1.DisksClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=DISK_FIELD_MAPPING,
            table_config=VM_DISKS_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Idle detection metrics
            MetricSpec(
                "compute.googleapis.com/instance/cpu/utilization",
                unit="Percent",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/cpu/utilization",
                unit="Percent",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/network/received_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/network/sent_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/read_ops_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/write_ops_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/read_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/write_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            # Overprovisioned detection metrics
            MetricSpec(
                "compute.googleapis.com/instance/memory/balloon/ram_used",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/memory/balloon/ram_used",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/network/received_bytes_count",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/network/sent_bytes_count",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/read_ops_count",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/write_ops_count",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/throttled_read_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "compute.googleapis.com/instance/disk/throttled_write_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
        ],
        resource_id_field="resource_id",
        table_config=VM_METRICS_TABLE,
    ),
)
