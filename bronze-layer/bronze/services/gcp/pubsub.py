from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

PUBSUB_SERVICE = GCPServiceCatalog(
    service_name="Pub/Sub",
    status_idle=[
        "pubsub.googleapis.com/topic/send_message_operation_count",
        "pubsub.googleapis.com/subscription/num_undelivered_messages",
        "pubsub.googleapis.com/subscription/oldest_unacked_message_age",
    ],
    status_overprovisioned=[
        "pubsub.googleapis.com/subscription/num_undelivered_messages",
        "pubsub.googleapis.com/subscription/oldest_unacked_message_age",
        "pubsub.googleapis.com/subscription/byte_cost",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="pubsub.googleapis.com/topic/send_message_operation_count",
            recommendation_type="Idle",
            signal="No messages published to topic",
            threshold="= 0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="pubsub.googleapis.com/subscription/num_undelivered_messages",
            recommendation_type="Idle",
            signal="Growing backlog — no consumer reading messages",
            threshold="Monotonically increasing over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="pubsub.googleapis.com/subscription/oldest_unacked_message_age",
            recommendation_type="Idle",
            signal="Messages unacknowledged for extended period",
            threshold="> 24 hours consistently",
        ),
        # --- Overprovisioned signals ---
        GCPMetricSignal(
            metric_type="pubsub.googleapis.com/subscription/num_undelivered_messages",
            recommendation_type="Overprovisioned",
            signal="Large backlog — consumer under-scaled",
            threshold="P95 > 10,000 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="pubsub.googleapis.com/subscription/oldest_unacked_message_age",
            recommendation_type="Overprovisioned",
            signal="Consumer not keeping up with message rate",
            threshold="P95 > 5 minutes (300 seconds)",
        ),
        GCPMetricSignal(
            metric_type="pubsub.googleapis.com/subscription/byte_cost",
            recommendation_type="Overprovisioned",
            signal="High byte volume — oversized message payloads",
            threshold="P95 > 10x expected baseline",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="Topic with no publishers for 14+ days",
            target_type="Any",
            recommended_action="Delete topic and all subscriptions",
            example="Delete unused topic",
            notes="Confirm no producers. Delivery billed per subscription — remove all dead subs.",
        ),
        GCPDecisionRule(
            finding="Subscription with growing unacked backlog",
            target_type="Pull/Push",
            recommended_action="Delete dead subscription",
            example="Delete dead subscription",
            notes="No consumer. Unacknowledged messages consume retention storage (default 7 days).",
        ),
        GCPDecisionRule(
            finding="oldest_unacked_message_age > 24h",
            target_type="Pull",
            recommended_action="Seek-to-time to reset backlog, fix consumer",
            example="Seek to current time",
            notes="Reset backlog to stop accumulating storage charges.",
        ),
        GCPDecisionRule(
            finding="Consumer not keeping up (oldest_unacked_message_age P95 > 5 min)",
            target_type="Pull/Push",
            recommended_action="Scale up consumer",
            example="Cloud Run min_instances: 1 -> 5 (increase replicas)",
            notes="",
        ),
        GCPDecisionRule(
            finding="Snapshot storage growing",
            target_type="Snapshot",
            recommended_action="Delete old snapshots",
            example="Expire snapshots after 7 days",
            notes="Set expiration policy.",
        ),
        GCPDecisionRule(
            finding="High byte_cost from large message payloads",
            target_type="Any",
            recommended_action="Reduce payload size — store data in GCS, send URI in message",
            example="10MB message -> GCS URI reference",
            notes="Delivery billed per GB per subscription. Large payloads multiply cost.",
        ),
        GCPDecisionRule(
            finding="Many subscriptions on high-volume topic",
            target_type="Any",
            recommended_action="Remove unused subscriptions",
            example="5 subscriptions -> 2 active subscriptions",
            notes="Each subscription receives a full copy — billed independently.",
        ),
    ],
    future_plans=[
        "Integrate percentile metrics into bronze layer.",
        "Ingest more granular intervals (1-, 5-, 10-minute).",
        "Update backend, frontend, and RDS for granularity.",
        "Use additional metrics as needed.",
    ],
    references=[
        "https://cloud.google.com/pubsub/docs/overview",
        "https://cloud.google.com/pubsub/pricing",
    ],
)
