from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

CLOUD_SCHEDULER_SERVICE = GCPServiceCatalog(
    service_name="Cloud Scheduler",
    status_idle=[
        "scheduler.googleapis.com/job/attempt_count",
        "scheduler.googleapis.com/job/error_count",
    ],
    status_overprovisioned=[
        "scheduler.googleapis.com/job/attempt_count",
        "scheduler.googleapis.com/job/attempt_dispatch_count",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="scheduler.googleapis.com/job/attempt_count",
            recommendation_type="Idle",
            signal="Job defined but never executing",
            threshold="= 0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="scheduler.googleapis.com/job/error_count",
            recommendation_type="Idle",
            signal="All executions failing — 0 successful completions",
            threshold="error_count = attempt_count (100% failure rate)",
        ),
        # --- Overprovisioned signals ---
        GCPMetricSignal(
            metric_type="scheduler.googleapis.com/job/attempt_count",
            recommendation_type="Overprovisioned",
            signal="Very high execution frequency — schedule too aggressive",
            threshold="Unusually high attempts relative to business need",
        ),
        GCPMetricSignal(
            metric_type="scheduler.googleapis.com/job/attempt_dispatch_count",
            recommendation_type="Overprovisioned",
            signal="Retry storm — dispatch_count >> attempt_count",
            threshold="dispatch_count >> attempt_count",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="Idle: no attempts for 14+ days",
            target_type="Any",
            recommended_action="Delete job",
            example="Delete orphaned job",
            notes="Check attempt_count. Always validate downstream impact before deletion.",
        ),
        GCPDecisionRule(
            finding="Idle: job paused",
            target_type="Any",
            recommended_action="Delete job",
            example="Delete paused job",
            notes="Paused jobs still incur full monthly billing — delete instead of pausing.",
        ),
        GCPDecisionRule(
            finding="Job targeting deleted resource (100% error rate)",
            target_type="HTTP/PubSub",
            recommended_action="Delete job; confirm endpoint",
            example="Delete job targeting 404 Cloud Run service or deleted Pub/Sub topic",
            notes="Check error_count. Job continues to run and fail, still incurring monthly charge.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: overlapping targets",
            target_type="Any",
            recommended_action="Consolidate or use fan-out",
            example="3 HTTP jobs -> 1 + PubSub",
            notes="From job config",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: very frequent schedule",
            target_type="Any",
            recommended_action="Reduce invocation frequency",
            example="Every 1 min -> every 15 min",
            notes="Validate vs business need",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: retry storm",
            target_type="Any",
            recommended_action="Tune retry settings",
            example="Reduce aggressive retries — monitor dispatch_count",
            notes="Non-idempotent endpoints require careful retry tuning.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: > 3 jobs in account",
            target_type="Any",
            recommended_action="Consolidate to stay within free tier (first 3 jobs free)",
            example="5 jobs -> 3 jobs",
            notes="First 3 jobs per billing account are free at $0.10/job/month beyond that.",
        ),
    ],
    future_plans=[
        "Integrate percentile metrics into bronze layer.",
        "Ingest more granular intervals (1-, 5-, 10-minute).",
        "Update backend, frontend, and RDS for granularity.",
        "Use additional metrics as needed.",
    ],
    references=[
        "https://cloud.google.com/scheduler/docs",
        "https://cloud.google.com/scheduler/pricing",
    ],
)
