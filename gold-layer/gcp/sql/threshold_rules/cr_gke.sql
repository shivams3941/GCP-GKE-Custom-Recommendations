-- Threshold Rules for GCP Google Kubernetes Engine (GKE)
-- Table: base_finops_threshold_rules_v2
-- Pattern: DELETE by service_code, then INSERT
-- Rules: GKE_IDLE, GKE_OVERPROVISIONED
-- Metrics: cpu_utilization_avg, memory_utilization_avg
--          (sourced from kubernetes.io/node/cpu/allocatable_utilization
--           and kubernetes.io/node/memory/allocatable_utilization via Cloud Monitoring)

DELETE FROM base_finops_threshold_rules_v2 WHERE service_code = 'GKE';

INSERT INTO base_finops_threshold_rules_v2 (
    rule_code,
    service_code,
    service_name,
    cloud_provider,
    evaluation_logic,
    category,
    title,
    description,
    recommendation_template,
    estimated_savings_formula,
    is_active
) VALUES
-- GKE_IDLE
(
    'GKE_IDLE',
    'GKE',
    'Google Kubernetes Engine',
    'gcp',
    '{"logic": "AND", "conditions": [{"metric": "cpu_utilization_avg", "operator": "lt", "threshold": 5}, {"metric": "memory_utilization_avg", "operator": "lt", "threshold": 20}]}',
    'Compute',
    'Idle GKE Node Pool',
    'This recommendation identifies GKE node pools with near-zero resource utilization. The analysis evaluates cpu_utilization_avg (< 5%) and memory_utilization_avg (< 20%) sourced from kubernetes.io/node/cpu/allocatable_utilization and kubernetes.io/node/memory/allocatable_utilization metrics over a 7-day observation window at 5-minute intervals. Node pools with sustained near-zero CPU and memory usage are considered idle and are candidates for deletion or scaling to 0 nodes to eliminate unnecessary compute costs. GKE Standard clusters bill per node, making idle node pool elimination highly impactful.',
    'Idle GKE node pool — delete or scale to 0 nodes (current: {current_sku})',
    'total_cost * 1',
    true
),
-- GKE_OVERPROVISIONED
(
    'GKE_OVERPROVISIONED',
    'GKE',
    'Google Kubernetes Engine',
    'gcp',
    '{"logic": "OR", "conditions": [{"metric": "cpu_utilization_avg", "operator": "lt", "threshold": 40}, {"metric": "memory_utilization_avg", "operator": "lt", "threshold": 50}]}',
    'Compute',
    'Overprovisioned GKE Node Pool',
    'This recommendation identifies GKE node pools where the machine type is larger than the workload requires. The analysis evaluates cpu_utilization_avg and memory_utilization_avg sourced from kubernetes.io/node/cpu/allocatable_utilization and kubernetes.io/node/memory/allocatable_utilization metrics over a 7-day observation window. Node pools are flagged as overprovisioned when CPU utilization is below 40% OR memory utilization is below 50%. The engine calculates required capacity with a 1.3x safety margin and recommends the closest cheaper machine type within the same family (e.g. e2, n1, n2, c2). Savings are calculated per node and multiplied by node count.',
    'Overprovisioned GKE node pool — rightsize from {current_sku} to {target_sku}',
    'current_cost - target_cost',
    true
);
