from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

CLOUD_STORAGE_SERVICE = GCPServiceCatalog(
    service_name="Cloud Storage",
    status_idle=[
        "storage.googleapis.com/api/request_count",
        "storage.googleapis.com/storage/total_bytes",
        "storage.googleapis.com/storage/object_count",
    ],
    status_overprovisioned=[
        "storage.googleapis.com/storage/total_bytes",
        "storage.googleapis.com/api/request_count",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="storage.googleapis.com/api/request_count",
            recommendation_type="Idle",
            signal="No ReadObject operations — no reads from bucket",
            threshold="ReadObject = 0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="storage.googleapis.com/api/request_count",
            recommendation_type="Idle",
            signal="No WriteObject operations — no writes to bucket",
            threshold="WriteObject = 0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="storage.googleapis.com/api/request_count",
            recommendation_type="Idle",
            signal="No ListObjects operations — no listing/access activity",
            threshold="ListObjects = 0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="storage.googleapis.com/storage/total_bytes",
            recommendation_type="Idle",
            signal="Storage flat — no new data ingested",
            threshold="Flat over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="storage.googleapis.com/storage/object_count",
            recommendation_type="Idle",
            signal="No object growth",
            threshold="Flat over 14-30 days",
        ),
        # --- Overprovisioned signals ---
        GCPMetricSignal(
            metric_type="storage.googleapis.com/api/request_count",
            recommendation_type="Overprovisioned",
            signal="Read frequency too low for current storage class",
            threshold="P95 < 1 ReadObject/month — candidate for colder class",
        ),
        GCPMetricSignal(
            metric_type="storage.googleapis.com/storage/total_bytes",
            recommendation_type="Overprovisioned",
            signal="Data growing without access — wrong storage class",
            threshold="Growing > 10% per week with zero reads",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="No reads + writes for 30+ days",
            target_type="Any",
            recommended_action="Apply lifecycle rule — transition to Nearline",
            example="Standard -> Nearline",
            notes="After idle period. Use Object Lifecycle Management or Autoclass.",
        ),
        GCPDecisionRule(
            finding="Bucket with zero objects",
            target_type="Any",
            recommended_action="Delete empty bucket",
            example="Delete bucket",
            notes="Remove unused resources.",
        ),
        GCPDecisionRule(
            finding="Access < once per month",
            target_type="Standard",
            recommended_action="Transition to Nearline",
            example="Standard -> Nearline",
            notes="Lifecycle rule. 30-day minimum duration applies.",
        ),
        GCPDecisionRule(
            finding="Access < once per quarter",
            target_type="Standard/Nearline",
            recommended_action="Transition to Coldline",
            example="Nearline -> Coldline",
            notes="90-day minimum duration applies.",
        ),
        GCPDecisionRule(
            finding="Compliance/no access expected",
            target_type="Any",
            recommended_action="Transition to Archive",
            example="Coldline -> Archive",
            notes="Lowest cost. 365-day minimum duration. Millisecond access still available.",
        ),
        GCPDecisionRule(
            finding="Objects older than retention window",
            target_type="Any",
            recommended_action="Set delete lifecycle rule",
            example="Delete after 365 days",
            notes="Automate with Object Lifecycle Management.",
        ),
        GCPDecisionRule(
            finding="Versioning enabled with old non-current versions",
            target_type="Any",
            recommended_action="Add lifecycle rule for non-current versions",
            example="Delete non-current versions after 30 days",
            notes="Non-current versions accumulate storage cost silently.",
        ),
    ],
    future_plans=[
        "Integrate percentile metrics into bronze layer.",
        "Ingest more granular intervals (1-, 5-, 10-minute).",
        "Update backend, frontend, and RDS for granularity.",
        "Add Autoclass status ingestion for automated transition tracking.",
    ],
    references=[
        "https://cloud.google.com/storage/docs/storage-classes",
        "https://cloud.google.com/storage/pricing",
        "https://cloud.google.com/storage/docs/lifecycle",
    ],
)
