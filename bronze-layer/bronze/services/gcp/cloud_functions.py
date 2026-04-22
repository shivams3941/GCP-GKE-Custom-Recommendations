from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

CLOUD_FUNCTIONS_SERVICE = GCPServiceCatalog(
    service_name="Cloud Functions",
    status_idle=[
        "cloudfunctions.googleapis.com/function/execution_count",
        "cloudfunctions.googleapis.com/function/active_instances",
        "cloudfunctions.googleapis.com/function/user_memory_bytes",
    ],
    status_overprovisioned=[
        "cloudfunctions.googleapis.com/function/user_memory_bytes",
        "cloudfunctions.googleapis.com/function/execution_times",
        "cloudfunctions.googleapis.com/function/active_instances",
        "cloudfunctions.googleapis.com/function/instance_count",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/execution_count",
            recommendation_type="Idle",
            signal="No function invocations",
            threshold="~0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/active_instances",
            recommendation_type="Idle",
            signal="No active instances — function never scales up",
            threshold="= 0 consistently over 14 days",
        ),
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/user_memory_bytes",
            recommendation_type="Idle",
            signal="No memory consumption — completely unused",
            threshold="= 0 or unobserved over 14 days",
        ),
        # --- Overprovisioned signals ---
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/user_memory_bytes",
            recommendation_type="Overprovisioned",
            signal="Memory headroom consistently large",
            threshold="P95 < 40% of provisioned memory over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/execution_times",
            recommendation_type="Overprovisioned",
            signal="Function completes well under configured timeout",
            threshold="P95 duration < 30% of configured timeout",
        ),
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/active_instances",
            recommendation_type="Overprovisioned",
            signal="Peak concurrency far below max_instances setting",
            threshold="Max active instances < 30% of max_instances config",
        ),
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/execution_count",
            recommendation_type="Overprovisioned",
            signal="min_instances > 0 but very low invocation rate — idle billing",
            threshold="Idle min_instances cost > 20% of total function cost",
        ),
        GCPMetricSignal(
            metric_type="cloudfunctions.googleapis.com/function/instance_count",
            recommendation_type="Overprovisioned",
            signal="Warm instances mostly idle",
            threshold="Idle ratio > 70% over 14 days",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="Idle: no executions + no active instances for 14+ days",
            target_type="Any",
            recommended_action="Delete or disable function",
            example="gcloud functions delete FUNCTION_NAME --region=REGION",
            notes="",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: memory P95 < 40% of provisioned (1st gen)",
            target_type="1st gen",
            recommended_action="Step down to smaller memory tier",
            example="1024 MB -> 512 MB (CPU: 0.2 -> 0.167 GHz implicitly)",
            notes="Memory and CPU are coupled on 1st gen — validate performance regression.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: memory P95 < 40% of provisioned (2nd gen)",
            target_type="2nd gen",
            recommended_action="Reduce memory allocation independently",
            example="2048 MB -> 1024 MB (CPU setting unchanged)",
            notes="Memory and CPU are independent on 2nd gen.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: CPU P95 < 25% (2nd gen only)",
            target_type="2nd gen",
            recommended_action="Reduce vCPU allocation",
            example="2 vCPU -> 1 vCPU",
            notes="Verify no latency regression before reducing CPU.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: min_instances > 0 with very low traffic",
            target_type="Any",
            recommended_action="Reduce or remove min_instances to eliminate idle billing",
            example="min_instances: 2 -> 0 or 1",
            notes="Accept cold start latency trade-off. Always flag cold start impact for latency-sensitive functions.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: timeout P95 < 30% of configured timeout",
            target_type="Any",
            recommended_action="Reduce timeout to limit runaway execution cost",
            example="540s timeout -> 60s (add safety margin above P99 duration)",
            notes="Reducing timeout does not reduce cost for normal executions but caps runaway ones.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: max_instances far above peak concurrency",
            target_type="Any",
            recommended_action="Lower max_instances to reduce over-scaling cost",
            example="max_instances: 100 -> 20 (set at 2x observed peak)",
            notes="",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: 1st gen function on legacy runtime",
            target_type="1st gen",
            recommended_action="Migrate to 2nd gen for independent CPU/memory control",
            example="python39 1st gen -> python312 2nd gen",
            notes="Review triggers and bindings before migration. Always prefer latest runtime version.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: CPU-bound workload, memory adequate (2nd gen)",
            target_type="2nd gen",
            recommended_action="Enable CPU always-on to avoid CPU throttle outside requests",
            example="Set cpu_idle = false for CPU-intensive background work",
            notes="",
        ),
    ],
    future_plans=[
        "Integrate percentile metrics into bronze layer.",
        "Ingest more granular intervals (1-, 5-, 10-minute).",
        "Update backend, frontend, and RDS for granularity.",
        "Incorporate 2nd gen Cloud Run-level CPU metrics for richer sizing signals.",
    ],
    references=[
        "https://cloud.google.com/functions/docs/concepts/overview",
        "https://cloud.google.com/functions/pricing",
        "https://cloud.google.com/monitoring/api/metrics_gcp#gcp-cloudfunctions",
    ],
)
