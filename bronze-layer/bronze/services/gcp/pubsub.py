"""GCP Pub/Sub service definition - pure config, no logic.

Declares how to fetch Pub/Sub topics and subscriptions from GCP,
and which Iceberg tables to write them to.

Notes:
- Pub/Sub manages messaging topics and subscriptions
- resource_id for topics: "project_id.topic_name"
- resource_id for subscriptions: "project_id.subscription_name"
- Metrics are collected from pubsub.googleapis.com namespace
- Idle signal: no message publish/subscribe activity over 14-30 days
- Overprovisioned signal: high message volume with dead-letter accumulation
"""

from google.cloud import pubsub_v1

from bronze.config.table_config import TableConfig
from bronze.services.base import MetricDefinition, MetricSpec, ResourceFetcher, ServiceDefinition

# ---------------------------------------------------------------------------
# Table configs
# ---------------------------------------------------------------------------

PUBSUB_TOPICS_TABLE = TableConfig(
    table_name="bronze_gcp_pubsub_topics",
    s3_path_suffix="gcp/pubsub_topics",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.topic_name
        "resource_name": "string",          # Topic display name
        "project_id": "string",
        "topic_name": "string",
        "kms_key_name": "string",
        "message_retention_duration": "string",
        "message_storage_policy": "string",  # Region binding for storage
        "labels": "map<string,string>",
        "schema_settings": "string",        # Schema configuration
        "message_ordering_enabled": "boolean",
        "snapshot_count": "int",
        "subscription_count": "int",
        "create_time": "string",
        "modify_time": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

PUBSUB_SUBSCRIPTIONS_TABLE = TableConfig(
    table_name="bronze_gcp_pubsub_subscriptions",
    s3_path_suffix="gcp/pubsub_subscriptions",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
        "resource_id": "string",            # project_id.subscription_name
        "resource_name": "string",          # Subscription display name
        "project_id": "string",
        "subscription_name": "string",
        "topic_name": "string",             # Parent topic
        "push_endpoint": "string",          # For push subscriptions
        "ack_deadline_seconds": "int",
        "message_retention_duration": "string",
        "retain_acked_messages": "boolean",
        "enable_message_ordering": "boolean",
        "filter": "string",                 # Subscription filter
        "dead_letter_policy": "string",     # Dead-letter topic configuration
        "retry_policy": "string",           # Retry configuration
        "detached": "boolean",              # Whether subscription is detached
        "enable_exactly_once_delivery": "boolean",
        "labels": "map<string,string>",
        "create_time": "string",
        "modify_time": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

PUBSUB_METRICS_TABLE = TableConfig(
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

PUBSUB_TOPIC_FIELD_MAPPING = {
    "name": "topic_name",
    "kms_key_name": "kms_key_name",
    "message_retention_duration": "message_retention_duration",
    "message_storage_policy": "message_storage_policy",
    "labels": "labels",
    "schema_settings": "schema_settings",
    "enable_message_ordering": "message_ordering_enabled",
    "create_time": "create_time",
    "modify_time": "modify_time",
}

PUBSUB_SUBSCRIPTION_FIELD_MAPPING = {
    "name": "subscription_name",
    "topic": "topic_name",
    "push_config.push_endpoint": "push_endpoint",
    "ack_deadline_seconds": "ack_deadline_seconds",
    "message_retention_duration": "message_retention_duration",
    "retain_acked_messages": "retain_acked_messages",
    "enable_message_ordering": "enable_message_ordering",
    "filter": "filter",
    "dead_letter_policy": "dead_letter_policy",
    "retry_policy": "retry_policy",
    "detached": "detached",
    "enable_exactly_once_delivery": "enable_exactly_once_delivery",
    "labels": "labels",
    "create_time": "create_time",
    "modify_time": "modify_time",
}

# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

PUBSUB_SERVICE = ServiceDefinition(
    name="Pub/Sub",
    namespace="pubsub.googleapis.com",
    resource_fetchers=[
        ResourceFetcher(
            sdk_client_class=pubsub_v1.PublisherClient,
            list_method="list_topics",
            field_mapping=PUBSUB_TOPIC_FIELD_MAPPING,
            table_config=PUBSUB_TOPICS_TABLE,
            composite_id_fields=("project_id", "topic_name"),
        ),
        ResourceFetcher(
            sdk_client_class=pubsub_v1.SubscriberClient,
            list_method="list_subscriptions",
            field_mapping=PUBSUB_SUBSCRIPTION_FIELD_MAPPING,
            table_config=PUBSUB_SUBSCRIPTIONS_TABLE,
            composite_id_fields=("project_id", "subscription_name"),
        ),
    ],
    metrics=MetricDefinition(
        metric_specs=[
            # Topic metrics
            MetricSpec("pubsub.googleapis.com/topic/message_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/topic/byte_count", unit="Bytes", aggregation="Sum", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/topic/snapshot_count", unit="Count", aggregation="Gauge", interval="PT5M"),
            
            # Subscription metrics
            MetricSpec("pubsub.googleapis.com/subscription/num_undelivered_messages", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/num_outstanding_messages", unit="Count", aggregation="Gauge", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/oldest_unacked_message_age", unit="Seconds", aggregation="Gauge", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/ack_message_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/nack_message_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/push_request_count", unit="Count", aggregation="Sum", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/push_request_latencies", unit="Milliseconds", aggregation="Mean", interval="PT5M"),
            
            # Dead letter metrics
            MetricSpec("pubsub.googleapis.com/subscription/dead_letter_message_count", unit="Count", aggregation="Sum", interval="PT5M"),
            
            # Backlog metrics
            MetricSpec("pubsub.googleapis.com/subscription/backlog_bytes", unit="Bytes", aggregation="Gauge", interval="PT5M"),
            
            # Flow control metrics
            MetricSpec("pubsub.googleapis.com/subscription/flow_control_limit_bytes", unit="Bytes", aggregation="Gauge", interval="PT5M"),
            MetricSpec("pubsub.googleapis.com/subscription/flow_control_limit_messages", unit="Count", aggregation="Gauge", interval="PT5M"),
        ],
        resource_id_field="resource_id",
        table_config=PUBSUB_METRICS_TABLE,
    ),
)
