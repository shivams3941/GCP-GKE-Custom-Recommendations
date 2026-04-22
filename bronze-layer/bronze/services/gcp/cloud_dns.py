from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

CLOUD_DNS_SERVICE = GCPServiceCatalog(
    service_name="Cloud DNS",
    status_idle=[
        "dns.googleapis.com/query_count",
        "dns.googleapis.com/record_set_count",
        "dns.googleapis.com/rrset_count",
    ],
    status_overprovisioned=[
        "dns.googleapis.com/query_count",
        "dns.googleapis.com/query_bytes_total",
        "dns.googleapis.com/response_latency",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="dns.googleapis.com/query_count",
            recommendation_type="Idle",
            signal="No DNS queries",
            threshold="~0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="dns.googleapis.com/record_set_count",
            recommendation_type="Idle",
            signal="Minimal records — zone likely unused",
            threshold="< 5 records + 0 queries over 30 days",
        ),
        GCPMetricSignal(
            metric_type="dns.googleapis.com/rrset_count",
            recommendation_type="Idle",
            signal="Underutilized capacity",
            threshold="Low count + no queries",
        ),
        GCPMetricSignal(
            metric_type="dns.googleapis.com/query_count",
            recommendation_type="Idle",
            signal="Core record types (A/AAAA/CNAME) unused",
            threshold="Primary record types = 0",
        ),
        # --- Overprovisioned signals ---
        GCPMetricSignal(
            metric_type="dns.googleapis.com/query_count",
            recommendation_type="Overprovisioned",
            signal="Excessive query volume — optimize delegation",
            threshold="> 10M queries/month",
        ),
        GCPMetricSignal(
            metric_type="dns.googleapis.com/query_bytes_total",
            recommendation_type="Overprovisioned",
            signal="High data transfer — repetitive query patterns",
            threshold="High bytes + repetitive patterns",
        ),
        GCPMetricSignal(
            metric_type="dns.googleapis.com/response_latency",
            recommendation_type="Overprovisioned",
            signal="Slow DNS responses",
            threshold="P95 > 100ms",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="Idle: query_count = 0 for 30+ days",
            target_type="Public",
            recommended_action="Delete DNS zone",
            example="gcloud dns managed-zones delete ZONE --project=PROJECT",
            notes="Validate GKE/Compute Engine dependencies first",
        ),
        GCPDecisionRule(
            finding="Idle: < 5 records + 0 queries",
            target_type="Any",
            recommended_action="Delete DNS zone",
            example="gcloud dns managed-zones delete oldzone.com",
            notes="",
        ),
        GCPDecisionRule(
            finding="Private zone with no VPC networks attached",
            target_type="Private",
            recommended_action="Delete zone",
            example="Validate no VPC peering/forwarding before deletion",
            notes="",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: > 10M queries/month",
            target_type="Public",
            recommended_action="Delegate to Cloud Load Balancer",
            example="Use LB with global anycast",
            notes="",
        ),
        GCPDecisionRule(
            finding="High query_count + repetitive patterns",
            target_type="Public",
            recommended_action="Implement wildcard records",
            example="*.example.com -> loadbalancer",
            notes="",
        ),
        GCPDecisionRule(
            finding="Orphaned forwarding targets unreachable",
            target_type="Forwarding",
            recommended_action="Delete or update forwarding targets",
            example="Fix unreachable IP targets",
            notes="",
        ),
        GCPDecisionRule(
            finding="Single-region query concentration (95%+ from 1 region)",
            target_type="Public",
            recommended_action="Add Cloud Load Balancer for regional routing",
            example="Regional LB + global anycast",
            notes="",
        ),
        GCPDecisionRule(
            finding="Private zone with no Private Service Connect",
            target_type="Private",
            recommended_action="Convert to public or delete",
            example="Link to single VPC or delete",
            notes="",
        ),
    ],
    future_plans=[
        "Integrate query_count percentiles into bronze layer.",
        "Add per-record-type query analysis.",
        "Update backend/frontend/RDS for VPC linking status.",
        "Add Private Service Connect integration.",
        "Cloud Load Balancer integration for high-volume delegation.",
    ],
    references=[
        "https://cloud.google.com/dns/docs/overview",
        "https://cloud.google.com/monitoring/api/metrics_gcp#gcp-dns",
        "https://cloud.google.com/dns/pricing",
    ],
)
