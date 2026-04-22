"""
GCP Pub/Sub Rightsizing Engine
================================

Handles two recommendation types for Pub/Sub topics and subscriptions:

  PUBSUB_IDLE
    - Topic has zero published messages over the observation window
    - OR subscription has a monotonically growing unacked backlog with no consumer
    - Recommendation: Delete topic or dead subscription
    - Savings = full current cost (delivery charges eliminated)

  PUBSUB_OVERPROVISIONED
    - Subscription has large unacked backlog (consumer under-scaled)
      (subscription/num_undelivered_messages P95 > 10,000 over 14-30 days)
    - OR oldest_unacked_message_age P95 > 5 minutes (consumer not keeping up)
    - OR subscription/byte_cost P95 > 10× expected baseline (large message payloads)
    - OR many subscriptions on high-volume topic (billed per subscription delivery)
    - Recommendation: Scale consumer, reduce payload size, or remove duplicate subscriptions

SKU catalog structure (pubsub_sku.json):
{
    "data_volume": {
        "first_10gb_monthly_free": true,
        "price_per_gb_after_free_tier": 0.04,
        "snapshot_storage_per_gb_month": 0.04,
        "seek_operations_per_million": 0.10
    }
}

Billing model:
  - Data volume: first 10 GB/month free, then $0.04/GB
  - Snapshot storage: $0.04/GB-month
  - Seek operations: $0.10/million
  - No per-topic or per-subscription fees
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

HOURS_PER_MONTH = 730
HOURS_PER_YEAR = 8760
MONTHS_PER_YEAR = 12

# Pub/Sub pricing constants (us-central1 / global)
FREE_TIER_GB_PER_MONTH = 10.0
PRICE_PER_GB = 0.04          # after free tier
SNAPSHOT_PRICE_PER_GB = 0.04
SEEK_PRICE_PER_MILLION = 0.10


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


class PubSubEngine(BaseGCPEngine):
    """GCP Pub/Sub rightsizing engine.

    Unlike compute engines, Pub/Sub has no SKU catalog with machine types.
    Cost is derived from data volume metrics stored in resource_data.
    The sku_catalog is loaded but used only for pricing constants.
    """

    def find_rightsize_candidate(
        self,
        current_sku: str,
        region: str,
        rule_code: str = "",
        recommendation_template: str = "",
        metrics: Dict[str, float] = None,
        resource_data: Dict = None,
        **kwargs,
    ) -> Optional[List]:
        return self._find_candidate(
            current_sku, region, rule_code, recommendation_template,
            metrics=metrics, resource_data=resource_data, **kwargs
        )

    def _find_candidate(
        self,
        current_sku: str,
        region: str,
        rule_code: str = "",
        recommendation_template: str = "",
        metrics: Dict[str, float] = None,
        resource_data: Dict = None,
        **kwargs,
    ) -> Optional[List]:
        metrics = metrics or {}
        resource_data = resource_data or {}

        if rule_code == "PUBSUB_IDLE":
            return self._handle_idle(current_sku, recommendation_template, metrics, resource_data)

        if rule_code == "PUBSUB_OVERPROVISIONED":
            return self._handle_overprovisioned(current_sku, recommendation_template, metrics, resource_data)

        logger.warning(f"Unknown rule_code: {rule_code}")
        return None

    # ------------------------------------------------------------------
    # IDLE handler
    # ------------------------------------------------------------------

    def _handle_idle(
        self,
        current_sku: str,
        recommendation_template: str,
        metrics: Dict[str, float],
        resource_data: Dict,
    ) -> Optional[List]:
        """
        Idle signal: send_message_operation_count = 0 over 14-30 days,
        or subscription has growing unacked backlog with no consumer.
        Savings = estimated monthly data cost * 12.
        """
        monthly_gb = _parse_num(resource_data.get("monthly_data_gb", 0))
        subscription_count = max(1, _parse_num(resource_data.get("subscription_count", 1)))

        # Billable GB = max(0, monthly_gb - free_tier) * subscription_count
        billable_gb = max(0.0, monthly_gb - FREE_TIER_GB_PER_MONTH) * subscription_count
        monthly_cost = round(billable_gb * PRICE_PER_GB, 4)
        annual_cost = round(monthly_cost * MONTHS_PER_YEAR, 2)

        recommendation = (
            recommendation_template
            or "Idle Pub/Sub topic — delete topic and all subscriptions (current: {current_sku})"
        ).format(current_sku=current_sku, target_sku="N/A")

        details = {
            "recommendation": recommendation,
            "resource_type": resource_data.get("resource_type", "topic"),
            "current_sku": current_sku,
            "monthly_data_gb": monthly_gb,
            "subscription_count": subscription_count,
            "billable_gb_per_month": billable_gb,
            "monthly_cost": monthly_cost,
            "annual_cost": annual_cost,
            "monthly_savings": monthly_cost,
            "annual_savings": annual_cost,
            "target_skus": [],
            "is_fallback": False,
        }

        logger.info(f"[PUBSUB_IDLE] {current_sku} -> annual_savings=${annual_cost}")
        return [recommendation, annual_cost, 0.0, annual_cost, details]

    # ------------------------------------------------------------------
    # OVERPROVISIONED handler
    # ------------------------------------------------------------------

    def _handle_overprovisioned(
        self,
        current_sku: str,
        recommendation_template: str,
        metrics: Dict[str, float],
        resource_data: Dict,
    ) -> Optional[List]:
        """
        Three overprovisioned signals per the documentation:

        Signal 1 — Large unacked backlog / consumer not keeping up:
          subscription/num_undelivered_messages P95 > 10,000
          subscription/oldest_unacked_message_age P95 > 5 minutes (300 seconds)
          Action: scale up consumer (Cloud Run min_instances, replicas)

        Signal 2 — High byte_cost from large message payloads:
          subscription/byte_cost P95 > 10× expected baseline
          Action: reduce payload size — store large data in GCS, send URI in message

        Signal 3 — Many subscriptions on high-volume topic:
          subscription_count > active_subscription_count
          Action: remove unused subscriptions (each billed per delivery copy)

        All three are evaluated; the most impactful action is surfaced first.
        """
        subscription_count = max(1, _parse_num(resource_data.get("subscription_count", 1)))
        active_subscriptions = max(1, _parse_num(resource_data.get("active_subscription_count", subscription_count)))
        monthly_gb = _parse_num(resource_data.get("monthly_data_gb", 0))

        # Signal 1 metrics
        undelivered_messages_p95 = _parse_num(metrics.get("num_undelivered_messages_p95", 0))
        oldest_unacked_age_p95_sec = _parse_num(metrics.get("oldest_unacked_message_age_p95_seconds", 0))

        # Signal 2: byte_cost P95 vs baseline
        # byte_cost_p95 is the observed P95 byte cost per interval
        # byte_cost_baseline is the expected baseline (e.g. median or configured value)
        byte_cost_p95 = _parse_num(metrics.get("byte_cost_p95", 0))
        byte_cost_baseline = _parse_num(metrics.get("byte_cost_baseline", 0))
        high_byte_cost = (
            byte_cost_baseline > 0
            and byte_cost_p95 > byte_cost_baseline * 10
        )

        # Signal 3: unused subscriptions
        unused_subs = max(0, subscription_count - active_subscriptions)

        # Determine primary action based on which signals fired
        signals_fired = []
        if undelivered_messages_p95 > 10_000:
            signals_fired.append(
                f"large unacked backlog (num_undelivered_messages P95={undelivered_messages_p95:,.0f} > 10,000)"
            )
        if oldest_unacked_age_p95_sec > 300:
            signals_fired.append(
                f"consumer not keeping up (oldest_unacked_message_age P95={oldest_unacked_age_p95_sec:.0f}s > 300s)"
            )
        if high_byte_cost:
            signals_fired.append(
                f"large message payloads (byte_cost P95={byte_cost_p95:.2f} > 10× baseline={byte_cost_baseline:.2f})"
            )
        if unused_subs > 0:
            signals_fired.append(
                f"{unused_subs} unused subscription(s) accumulating delivery charges"
            )

        if not signals_fired:
            # Fallback: flag subscription count as potential savings lever
            signals_fired.append("multiple subscriptions on topic — review for unused delivery copies")

        # Cost calculation: savings from removing unused subscriptions (Signal 3)
        # This is the most directly quantifiable saving lever
        billable_gb_current = max(0.0, monthly_gb - FREE_TIER_GB_PER_MONTH) * subscription_count
        billable_gb_target = max(0.0, monthly_gb - FREE_TIER_GB_PER_MONTH) * active_subscriptions

        monthly_cost_current = round(billable_gb_current * PRICE_PER_GB, 4)
        monthly_cost_target = round(billable_gb_target * PRICE_PER_GB, 4)
        monthly_savings = round(monthly_cost_current - monthly_cost_target, 4)
        annual_cost_current = round(monthly_cost_current * MONTHS_PER_YEAR, 2)
        annual_cost_target = round(monthly_cost_target * MONTHS_PER_YEAR, 2)
        annual_savings = round(annual_cost_current - annual_cost_target, 2)

        # Build action description
        actions = []
        if undelivered_messages_p95 > 10_000 or oldest_unacked_age_p95_sec > 300:
            actions.append("scale up consumer (increase Cloud Run min_instances or replicas)")
        if high_byte_cost:
            actions.append("reduce payload size — store large data in GCS, send URI in message instead")
        if unused_subs > 0:
            actions.append(f"remove {unused_subs} unused subscription(s) to eliminate duplicate delivery charges")

        action_str = "; ".join(actions) if actions else "review subscription configuration"
        target_sku = f"{active_subscriptions} active subscription(s)"

        recommendation = (
            recommendation_template
            or "Overprovisioned Pub/Sub {current_sku} — {action}"
        ).format(
            current_sku=current_sku,
            target_sku=target_sku,
            action=action_str,
        )

        details = {
            "recommendation": recommendation,
            "resource_type": resource_data.get("resource_type", "topic"),
            "current_sku": current_sku,
            "monthly_data_gb": monthly_gb,
            "subscription_count": subscription_count,
            "active_subscription_count": active_subscriptions,
            "unused_subscriptions": unused_subs,
            "signals_fired": signals_fired,
            "undelivered_messages_p95": undelivered_messages_p95,
            "oldest_unacked_age_p95_seconds": oldest_unacked_age_p95_sec,
            "byte_cost_p95": byte_cost_p95,
            "byte_cost_baseline": byte_cost_baseline,
            "high_byte_cost": high_byte_cost,
            "monthly_cost_current": monthly_cost_current,
            "monthly_cost_target": monthly_cost_target,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
            "target_skus": [{"label": target_sku, "monthly_cost": monthly_cost_target}],
            "is_fallback": False,
        }

        logger.info(
            f"[PUBSUB_OVERPROV] {current_sku}: signals={signals_fired}, "
            f"{subscription_count} subs -> {active_subscriptions} subs, annual_savings=${annual_savings}"
        )
        return [recommendation, annual_cost_current, annual_cost_target, annual_savings, details]
