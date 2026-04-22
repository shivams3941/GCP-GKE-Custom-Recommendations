"""GCP Cloud Router service definition — pure config, no logic.

Declares how to fetch Cloud Routers and their BGP sessions from GCP,
and which Iceberg tables to write them to.

Authentication:
- GCP credentials are loaded at runtime via bronze.auth.gcp_auth.get_gcp_credentials()
- The service account key JSON is stored in AWS Secrets Manager under
  the secret name defined in GCP_SECRET_NAME (gcp/devops-internal/service-account)

Notes:
- Cloud Routers are regional resources fetched via aggregated_list across all regions.
- BGP peers (sessions) are nested inside each router's bgpPeers list — they are
  extracted as child records using parent_id_source on the BGP peers ResourceFetcher.
- resource_id for routers is composed as "project_id.region.router_name".
- resource_id for BGP peers is composed as "project_id.region.router_name.peer_name".
- Cloud Router itself has no per-hour charge; cost is driven by attached transport
  (VPN tunnel, Interconnect VLAN attachment, or Router Appliance instance).
- Cloud NAT metrics are handled by the CloudNAT service; this service focuses on
  BGP session health and router-level idle/overprovisioned signals.

Metrics covered:
  Idle:          bgp/session_up, nat/allocated_ports
  Overprovisioned: nat/port_usage, nat/nat_ip_count
"""

from google.cloud import compute_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

CLOUDROUTER_ROUTERS_TABLE = TableConfig(
    table_name="bronze_gcp_cloudrouter_routers",
    s3_path_suffix="gcp/cloudrouter_routers",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.region.router_name
        "resource_name": "string",
        "project_id": "string",
        "region": "string",
        "router_name": "string",
        "description": "string",
        "network": "string",                # VPC network self-link
        "bgp_asn": "double",                # local BGP ASN
        "bgp_advertise_mode": "string",     # DEFAULT or CUSTOM
        "bgp_advertised_groups": "string",  # JSON list of advertised route groups
        "bgp_advertised_ip_ranges": "string",  # JSON list of custom advertised prefixes
        "encrypted_interconnect_router": "string",  # "true" / "false"
        "creation_timestamp": "string",
        "self_link": "string",
        "kind": "string",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDROUTER_BGP_PEERS_TABLE = TableConfig(
    table_name="bronze_gcp_cloudrouter_bgp_peers",
    s3_path_suffix="gcp/cloudrouter_bgp_peers",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.region.router_name.peer_name
        "resource_name": "string",          # peer_name
        "project_id": "string",
        "region": "string",
        "router_name": "string",
        "peer_name": "string",
        "peer_asn": "double",               # remote BGP ASN
        "interface_name": "string",         # router interface this peer is attached to
        "ip_address": "string",             # local BGP IP
        "peer_ip_address": "string",        # remote BGP IP
        "advertised_route_priority": "double",  # MED value
        "advertise_mode": "string",         # DEFAULT or CUSTOM
        "management_type": "string",        # MANAGED_BY_USER or MANAGED_BY_ATTACHMENT
        "enable": "string",                 # "true" / "false" — peer enabled flag
        "enable_ipv6": "string",            # "true" / "false"
        "bfd_session_initialization_mode": "string",  # ACTIVE, PASSIVE, DISABLED
        "bfd_min_transmit_interval": "double",
        "bfd_min_receive_interval": "double",
        "bfd_multiplier": "double",
        "service_name": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

CLOUDROUTER_METRICS_TABLE = TableConfig(
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

CLOUDROUTER_ROUTER_FIELD_MAPPING = {
    "name": "router_name",
    "description": "description",
    "region": "region",
    "network": "network",
    "bgp.asn": "bgp_asn",
    "bgp.advertise_mode": "bgp_advertise_mode",
    "bgp.advertised_groups": "bgp_advertised_groups",
    "bgp.advertised_ip_ranges": "bgp_advertised_ip_ranges",
    "encrypted_interconnect_router": "encrypted_interconnect_router",
    "creation_timestamp": "creation_timestamp",
    "self_link": "self_link",
    "kind": "kind",
}

CLOUDROUTER_BGP_PEER_FIELD_MAPPING = {
    "name": "peer_name",
    "peer_asn": "peer_asn",
    "interface_name": "interface_name",
    "ip_address": "ip_address",
    "peer_ip_address": "peer_ip_address",
    "advertised_route_priority": "advertised_route_priority",
    "advertise_mode": "advertise_mode",
    "management_type": "management_type",
    "enable": "enable",
    "enable_ipv6": "enable_ipv6",
    "bfd.session_initialization_mode": "bfd_session_initialization_mode",
    "bfd.min_transmit_interval": "bfd_min_transmit_interval",
    "bfd.min_receive_interval": "bfd_min_receive_interval",
    "bfd.multiplier": "bfd_multiplier",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

CLOUDROUTER_SERVICE = ServiceDefinition(
    name="CloudRouter",
    namespace="router.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=compute_v1.RoutersClient,
            list_method="aggregated_list",
            fallback_list_method="list",
            field_mapping=CLOUDROUTER_ROUTER_FIELD_MAPPING,
            table_config=CLOUDROUTER_ROUTERS_TABLE,
            composite_id_fields=("project_id", "region", "router_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # --- Idle detection ---
            # BGP session down — neighbour not established (= 0 for 7+ days)
            MetricSpec(
                "router.googleapis.com/bgp/session_up",
                unit="Bool",
                aggregation="Average",
                interval="PT1H",
            ),
            # No routes received from peer (= 0 over 7–14 days)
            MetricSpec(
                "router.googleapis.com/bgp/received_routes_count",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # No routes sent to peer (= 0 over 7–14 days)
            MetricSpec(
                "router.googleapis.com/bgp/sent_routes_count",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # NAT ports allocated but no active connections (= 0 over 14 days)
            MetricSpec(
                "router.googleapis.com/nat/allocated_ports",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # --- Overprovisioned detection ---
            # NAT port usage P95 — well below allocation means min-ports-per-VM too high
            MetricSpec(
                "router.googleapis.com/nat/port_usage",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
            MetricSpec(
                "router.googleapis.com/nat/port_usage",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # Excess NAT IPs — P95 utilisation < 30% of total NAT IP capacity
            MetricSpec(
                "router.googleapis.com/nat/nat_ip_count",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            MetricSpec(
                "router.googleapis.com/nat/nat_ip_count",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
            # Packets dropped due to NAT exhaustion (under-provisioning signal)
            MetricSpec(
                "router.googleapis.com/nat/dropped_sent_packets_count",
                unit="Count",
                aggregation="Average",
                interval="PT1H",
            ),
            # BGP route count P95 — excessive routes from peer (> 10k with < 5% utilisation)
            MetricSpec(
                "router.googleapis.com/bgp/received_routes_count",
                unit="Count",
                aggregation="Percentile",
                percentile=95,
                interval="PT1H",
            ),
        ],
        resource_id_field="resource_id",
        table_config=CLOUDROUTER_METRICS_TABLE,
    ),
)
