from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

CLOUD_CDN_SERVICE = GCPServiceCatalog(
    service_name="Cloud CDN",
    status_idle=[
        "loadbalancing.googleapis.com/https/request_count",
        "loadbalancing.googleapis.com/https/backend_request_count",
        "serving.googleapis.com/cdn/cache_hit_ratio",
        "cloud_load_balancing.googleapis.com/edge/cache_hit_ratio",
        "loadbalancing.googleapis.com/https/sent_bytes_count",
    ],
    status_overprovisioned=[
        "serving.googleapis.com/cdn/cache_hit_ratio",
        "loadbalancing.googleapis.com/https/backend_request_count",
        "loadbalancing.googleapis.com/https/backend_latencies",
        "loadbalancing.googleapis.com/https/total_egress_bytes",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="loadbalancing.googleapis.com/https/request_count",
            recommendation_type="Idle",
            signal="No traffic hitting CDN",
            threshold="~0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="loadbalancing.googleapis.com/https/backend_request_count",
            recommendation_type="Idle",
            signal="Origin never reached",
            threshold="No backend requests for 30 days",
        ),
        GCPMetricSignal(
            metric_type="serving.googleapis.com/cdn/cache_hit_ratio",
            recommendation_type="Idle",
            signal="CDN ineffective — cache never used",
            threshold="~0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="cloud_load_balancing.googleapis.com/edge/cache_hit_ratio",
            recommendation_type="Idle",
            signal="Cache never used at edge",
            threshold="0% consistently over 30 days",
        ),
        GCPMetricSignal(
            metric_type="loadbalancing.googleapis.com/https/sent_bytes_count",
            recommendation_type="Idle",
            signal="No egress served from CDN",
            threshold="~0 bytes over 14 days",
        ),
        # --- Overprovisioned signals ---
        GCPMetricSignal(
            metric_type="serving.googleapis.com/cdn/cache_hit_ratio",
            recommendation_type="Overprovisioned",
            signal="Poor caching — CDN not effective",
            threshold="P50 < 80% over 30 days",
        ),
        GCPMetricSignal(
            metric_type="loadbalancing.googleapis.com/https/backend_request_count",
            recommendation_type="Overprovisioned",
            signal="High cache miss rate — excessive origin load",
            threshold=">20% of requests are cache misses",
        ),
        GCPMetricSignal(
            metric_type="loadbalancing.googleapis.com/https/backend_latencies",
            recommendation_type="Overprovisioned",
            signal="Slow origins on cache misses",
            threshold="P95 > 500ms on misses",
        ),
        GCPMetricSignal(
            metric_type="loadbalancing.googleapis.com/https/total_egress_bytes",
            recommendation_type="Overprovisioned",
            signal="Low CDN savings — CDN bytes < 30% of total egress",
            threshold="CDN bytes < 30% total egress",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="Idle: zero requests for 30+ days",
            target_type="Any",
            recommended_action="Delete CDN origin",
            example="gcloud compute backend-services remove-backend ORIGIN --load-balancing-scheme=EXTERNAL_MANAGED",
            notes="Validate no active traffic before deleting",
        ),
        GCPDecisionRule(
            finding="Idle: zero edge bytes served",
            target_type="Any",
            recommended_action="Disable CDN on backend service",
            example="gcloud compute backend-services update BS --no-enable-cdn",
            notes="",
        ),
        GCPDecisionRule(
            finding="Cache hit ratio < 30% (static content)",
            target_type="CACHE_ALL_STATIC",
            recommended_action="Force aggressive caching",
            example="Cache-Control: public, max-age=31536000 on origin",
            notes="Test cache mode changes in staging first",
        ),
        GCPDecisionRule(
            finding="Cache hit ratio < 30% (dynamic content)",
            target_type="CACHE_ALL_DYNAMIC",
            recommended_action="Optimize cache keys",
            example="Add Vary: Accept + longer TTL for 200/304",
            notes="",
        ),
        GCPDecisionRule(
            finding="Frequent purges > 10/day",
            target_type="Any",
            recommended_action="Reduce purge frequency",
            example="Batch invalidations, use signed URLs",
            notes="",
        ),
        GCPDecisionRule(
            finding="Origin latency >> edge latency",
            target_type="Any",
            recommended_action="Tune origin TTL/cache mode",
            example="Increase default_max_allowed_ttl to 1 hour",
            notes="",
        ),
    ],
    future_plans=[
        "Integrate cache_hit_ratio percentiles into bronze layer.",
        "Ingest per-path cache hit ratios for granular recommendations.",
        "Update backend/frontend/RDS for cache mode/policy status.",
        "Add signed URL/cookie detection for cache-key optimization.",
    ],
    references=[
        "https://cloud.google.com/cdn/docs/overview",
        "https://cloud.google.com/monitoring/api/metrics_gcp#gcp-loadbalancing",
        "https://oneuptime.com/blog/post/2026-02-17-how-to-monitor-cloud-load-balancer-metrics-and-set-up-latency-alerts/view",
    ],
)
