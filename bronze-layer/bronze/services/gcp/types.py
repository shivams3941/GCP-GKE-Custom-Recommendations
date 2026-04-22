from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class GCPMetricSignal:
    metric_type: str
    recommendation_type: str
    signal: str
    threshold: str


@dataclass(frozen=True)
class GCPDecisionRule:
    finding: str
    target_type: str
    recommended_action: str
    example: str
    notes: str


@dataclass(frozen=True)
class GCPServiceCatalog:
    service_name: str
    status_idle: List[str]
    status_overprovisioned: List[str]
    metrics: List[GCPMetricSignal]
    decision_rules: List[GCPDecisionRule]
    future_plans: List[str]
    references: List[str]
