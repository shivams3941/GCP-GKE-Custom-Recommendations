from bronze.services.gcp.types import GCPDecisionRule, GCPMetricSignal, GCPServiceCatalog

ARTIFACT_REGISTRY_SERVICE = GCPServiceCatalog(
    service_name="Artifact Registry",
    status_idle=[
        "artifactregistry.googleapis.com/repository/request_count",
        "artifactregistry.googleapis.com/repository/api/request_count",
        "artifactregistry.googleapis.com/repository/size",
    ],
    status_overprovisioned=[
        "artifactregistry.googleapis.com/repository/size",
        "artifactregistry.googleapis.com/project/request_count",
    ],
    metrics=[
        # --- Idle signals ---
        GCPMetricSignal(
            metric_type="artifactregistry.googleapis.com/repository/request_count",
            recommendation_type="Idle",
            signal="No repository pull/push activity",
            threshold="Sum = 0 or near 0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="artifactregistry.googleapis.com/repository/api/request_count",
            recommendation_type="Idle",
            signal="No repository admin activity",
            threshold="~0 over 14-30 days",
        ),
        GCPMetricSignal(
            metric_type="artifactregistry.googleapis.com/repository/size",
            recommendation_type="Idle",
            signal="Repository empty or unchanged and unused",
            threshold="Size = 0, or very small and flat, with near-zero requests for 14-30 days",
        ),
        # --- Overprovisioned / storage bloat signals ---
        GCPMetricSignal(
            metric_type="artifactregistry.googleapis.com/repository/size",
            recommendation_type="Overprovisioned",
            signal="Large retained storage with very low access",
            threshold="High size with very low request_count over 30 days",
        ),
        GCPMetricSignal(
            metric_type="artifactregistry.googleapis.com/project/request_count",
            recommendation_type="Overprovisioned",
            signal="Low overall project usage compared with stored footprint",
            threshold="Low request volume for 30 days while repository storage remains high",
        ),
    ],
    decision_rules=[
        GCPDecisionRule(
            finding="Idle: no requests + no pulls + no pushes for 30+ days",
            target_type="Any",
            recommended_action="Delete repository or archive contents and delete",
            example="gcloud artifacts repositories delete REPO --location=REGION",
            notes="Validate no active service, CI pipeline, or deployment references this repo before deletion.",
        ),
        GCPDecisionRule(
            finding="Idle: storage present but zero activity (Docker)",
            target_type="Docker",
            recommended_action="Run cleanup policy (untagged + old tags), then delete if still unused",
            example="Set cleanup policy: keep latest 3 tags, delete untagged after 7 days",
            notes="Check cleanup policy status before recommending manual deletion.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: old image versions never pulled (> 60 days)",
            target_type="Docker",
            recommended_action="Enable tag-based cleanup policy or delete stale digests",
            example="Cleanup policy: delete versions with 0 pulls older than 60 days",
            notes="Deleting a Docker repository permanently removes all images — confirm no consumers.",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: untagged/dangling layers > 20% of storage",
            target_type="Docker",
            recommended_action="Delete untagged images via cleanup policy",
            example="Cleanup policy: delete untagged images after 1 day",
            notes="",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: Maven/npm versions never downloaded",
            target_type="Maven/npm",
            recommended_action="Delete unpulled versions older than retention window",
            example="Remove artifact versions with 0 downloads for > 90 days",
            notes="",
        ),
        GCPDecisionRule(
            finding="Overprovisioned: large Generic repo with infrequent access",
            target_type="Generic",
            recommended_action="Move artifacts to Cloud Storage (cheaper blob storage tier)",
            example="Migrate binaries to GCS Nearline/Coldline based on access frequency",
            notes="Multi-region repositories cannot be downgraded to regional — requires new repo + re-push.",
        ),
    ],
    future_plans=[
        "Integrate percentile metrics into bronze layer.",
        "Ingest per-image-version pull counts at finer granularity.",
        "Update backend, frontend, and RDS for cleanup policy status ingestion.",
        "Use vulnerability scanning severity as a signal for unused + vulnerable image deletion.",
    ],
    references=[
        "https://docs.cloud.google.com/monitoring/api/metrics_gcp_a_b#gcp-artifactregistry",
        "https://docs.cloud.google.com/artifact-registry/docs/supported-formats",
        "https://docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy-overview",
    ],
)
