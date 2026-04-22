"""GCP Cloud NAT service definition — pure config, no logic.

Declares how to fetch Cloud NAT gateways and metrics from GCP,
and which Iceberg tables to write them to.
"""

from google.cloud import compute_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUDNAT_GATEWAYS_TABLE = TableConfig(
    table_name="bronze_gcp_cloudnat_gateways",
    s3_path_suffix="gcp/cloudnat_gateways",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",
        "resource_name": "string",
        "region": "string",
        "nat_ip_allocate_option": "string",
        "source_subnetwork_ip_ranges_to_nat": "string",
        "nat_ips": "string",
        "min_ports_per_vm": "int",
        "max_ports_per_vm": "int",
        "enable_dynamic_port_allocation": "boolean",
        "enable_endpoint_independent_mapping": "boolean",
        "log_config_enable": "boolean",
        "log_config_filter": "string",
        "subnetworks": "string",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDNAT_METRICS_TABLE = TableConfig(
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

CLOUDNAT_FIELD_MAPPING = {
    "id": "resource_id",
    "name": "resource_name",
    "region": "region",
    "nat_ip_allocate_option": "nat_ip_allocate_option",
    "source_subnetwork_ip_ranges_to_nat": "source_subnetwork_ip_ranges_to_nat",
    "nat_ips": "nat_ips",
    "min_ports_per_vm": "min_ports_per_vm",
    "max_ports_per_vm": "max_ports_per_vm",
    "enable_dynamic_port_allocation": "enable_dynamic_port_allocation",
    "enable_endpoint_independent_mapping": "enable_endpoint_independent_mapping",
    "log_config.enable": "log_config_enable",
    "log_config.filter": "log_config_filter",
    "subnetworks": "subnetworks",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUDNAT_SERVICE = ServiceDefinition(
    name="CloudNAT",
    namespace="compute.googleapis.com/Router",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=compute_v1.RoutersClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=CLOUDNAT_FIELD_MAPPING,
            table_config=CLOUDNAT_GATEWAYS_TABLE,
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Idle detection metrics
            MetricSpec(
                "router.googleapis.com/nat/sent_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/received_bytes_count",
                unit="Bytes",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/open_connections",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/allocated_ports",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/closed_connections_count",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
            # Overprovisioned detection metrics
            MetricSpec(
                "router.googleapis.com/nat/sent_bytes_count",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/received_bytes_count",
                unit="Bytes",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/open_connections",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/allocated_ports",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/port_usage",
                unit="Percent",
                aggregation="Average",
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/port_usage",
                unit="Percent",
                aggregation="Percentile",
                percentile=95,
                interval="PT5M",
            ),
            MetricSpec(
                "router.googleapis.com/nat/nat_allocation_failed",
                unit="Count",
                aggregation="Average",
                interval="PT5M",
            ),
        ],
        resource_id_field="resource_id",
        table_config=CLOUDNAT_METRICS_TABLE,
    ),
)
