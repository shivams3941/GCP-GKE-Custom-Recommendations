"""
GCP Cloud DNS Rightsizing Engine
===================================

Handles two recommendation types for Cloud DNS managed zones:

  CLOUDDNS_IDLE
    - Zone has zero or near-zero query_count over 14-30 days
    - OR zone has < 5 records and no queries
    - OR Zone update_time (metadata) > 90 days since last update with no traffic
      (this is a metadata signal, not a Cloud Monitoring metric)
    - Recommendation: Delete managed zone
    - Savings = zone fee ($0.20/zone/month) + query cost eliminated

  CLOUDDNS_OVERPROVISIONED
    - Zone receives > 10M queries/month (delegation opportunity)
    - OR orphaned forwarding zone with unreachable targets
    - OR single-region query concentration (Cloud Load Balancer opportunity)
    - Recommendation: Delegate to Cloud Load Balancer, add wildcard records,
      or fix/delete orphaned forwarding targets

SKU catalog structure (clouddns_sku.json):
{
    "pricing": {
        "zone_fee_per_month": 0.20,
        "free_zones_per_account": 25,
        "query_price_tier1_per_million": 0.40,
        "query_price_tier2_per_million": 0.20,
        "query_tier1_limit_millions": 1000
    }
}

Billing model:
  - $0.20/zone/month (first 25 zones free per billing account)
  - Queries: $0.40/million (first 1B), $0.20/million (above 1B)
  - Private zones: zone fee only (no query charges)
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

MONTHS_PER_YEAR = 12

# Cloud DNS pricing
ZONE_FEE_PER_MONTH = 0.20
FREE_ZONES_PER_ACCOUNT = 25
QUERY_PRICE_TIER1_PER_MILLION = 0.40   # first 1B queries/month
QUERY_PRICE_TIER2_PER_MILLION = 0.20   # above 1B queries/month
QUERY_TIER1_LIMIT_MILLIONS = 1_000     # 1 billion = 1000 million


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _calculate_query_cost_monthly(query_count_millions: float) -> float:
    """Calculate monthly query cost based on tiered pricing."""
    if query_count_millions <= 0:
        return 0.0
    tier1 = min(query_count_millions, QUERY_TIER1_LIMIT_MILLIONS)
    tier2 = max(0.0, query_count_millions - QUERY_TIER1_LIMIT_MILLIONS)
    return round(
        tier1 * QUERY_PRICE_TIER1_PER_MILLION + tier2 * QUERY_PRICE_TIER2_PER_MILLION,
        4,
    )


class CloudDNSEngine(BaseGCPEngine):
    """GCP Cloud DNS managed zone rightsizing engine."""

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

        if rule_code == "CLOUDDNS_IDLE":
            return self._handle_idle(current_sku, recommendation_template, metrics, resource_data)

        if rule_code == "CLOUDDNS_OVERPROVISIONED":
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
        Idle: query_count ≈ 0 over 14-30 days, or < 5 records with no queries.
        Savings = zone fee + query cost (if public zone).
        """
        zone_type = (resource_data.get("zone_type") or "PUBLIC").upper()
        zone_ordinal = _parse_num(resource_data.get("zone_ordinal", FREE_ZONES_PER_ACCOUNT + 1))
        total_zones = _parse_num(resource_data.get("total_zones_in_account", FREE_ZONES_PER_ACCOUNT + 1))

        # Zone fee: only billable if beyond the 25 free zones
        is_billable_zone = zone_ordinal > FREE_ZONES_PER_ACCOUNT
        zone_monthly = ZONE_FEE_PER_MONTH if is_billable_zone else 0.0

        # Query cost: only public zones are billed for queries
        query_count_millions = _parse_num(metrics.get("query_count_millions", 0))
        query_monthly = _calculate_query_cost_monthly(query_count_millions) if zone_type == "PUBLIC" else 0.0

        monthly_cost = round(zone_monthly + query_monthly, 4)
        annual_cost = round(monthly_cost * MONTHS_PER_YEAR, 2)

        record_count = _parse_num(resource_data.get("record_set_count", 0))
        # days_since_last_update comes from zone metadata (update_time field),
        # not from Cloud Monitoring — the fetch script must populate this from
        # the DNS API managedZone.creationTime / update_time metadata field.
        days_since_update = _parse_num(resource_data.get("days_since_last_update", 0))

        if record_count < 5 and query_count_millions == 0:
            reason = f"< 5 records ({int(record_count)}) and zero queries"
        elif days_since_update > 90 and query_count_millions == 0:
            reason = f"no zone updates in {int(days_since_update)} days (update_time metadata) and zero queries"
        else:
            reason = "zero or near-zero query volume over 14-30 days"

        recommendation = (
            recommendation_template
            or "Idle Cloud DNS zone — delete {current_sku} ({reason})"
        ).format(current_sku=current_sku, target_sku="N/A", reason=reason)

        details = {
            "recommendation": recommendation,
            "zone_name": current_sku,
            "zone_type": zone_type,
            "record_set_count": record_count,
            "days_since_last_update": days_since_update,
            "query_count_millions_per_month": query_count_millions,
            "is_billable_zone": is_billable_zone,
            "zone_monthly_fee": zone_monthly,
            "query_monthly_cost": query_monthly,
            "monthly_cost": monthly_cost,
            "annual_cost": annual_cost,
            "monthly_savings": monthly_cost,
            "annual_savings": annual_cost,
            "reason": reason,
            "target_skus": [],
            "is_fallback": False,
        }

        logger.info(f"[CLOUDDNS_IDLE] {current_sku}: {reason}, annual_savings=${annual_cost}")
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
        Overprovisioned: high query volume (> 10M/month) or orphaned forwarding zone.
        Primary lever: delegate to Cloud Load Balancer for high-volume zones,
        or delete/fix orphaned forwarding targets.
        """
        zone_type = (resource_data.get("zone_type") or "PUBLIC").upper()
        query_count_millions = _parse_num(metrics.get("query_count_millions", 0))

        query_monthly_current = _calculate_query_cost_monthly(query_count_millions) if zone_type == "PUBLIC" else 0.0
        zone_monthly = ZONE_FEE_PER_MONTH  # assume billable for overprovisioned analysis
        monthly_cost_current = round(zone_monthly + query_monthly_current, 4)
        annual_cost_current = round(monthly_cost_current * MONTHS_PER_YEAR, 2)

        is_orphaned_forwarding = (
            zone_type == "FORWARDING"
            and _parse_num(resource_data.get("forwarding_targets_reachable", 1)) == 0
        )

        if is_orphaned_forwarding:
            # Orphaned forwarding zone — delete it
            recommendation = (
                recommendation_template
                or "Overprovisioned Cloud DNS — delete orphaned forwarding zone {current_sku} (targets unreachable)"
            ).format(current_sku=current_sku, target_sku="N/A")

            details = {
                "recommendation": recommendation,
                "zone_name": current_sku,
                "zone_type": zone_type,
                "action": "delete_orphaned_forwarding_zone",
                "query_count_millions_per_month": query_count_millions,
                "monthly_cost_current": monthly_cost_current,
                "monthly_cost_target": 0.0,
                "monthly_savings": monthly_cost_current,
                "annual_savings": annual_cost_current,
                "target_skus": [],
                "is_fallback": False,
            }
            return [recommendation, annual_cost_current, 0.0, annual_cost_current, details]

        if query_count_millions > 10:
            # High query volume — recommend wildcard records or LB delegation
            # Wildcard records reduce unique query volume by consolidating lookups
            # Estimated 30% query reduction from wildcard optimization
            optimized_millions = round(query_count_millions * 0.70, 2)
            query_monthly_target = _calculate_query_cost_monthly(optimized_millions)
            monthly_cost_target = round(zone_monthly + query_monthly_target, 4)
            annual_cost_target = round(monthly_cost_target * MONTHS_PER_YEAR, 2)
            monthly_savings = round(monthly_cost_current - monthly_cost_target, 4)
            annual_savings = round(annual_cost_current - annual_cost_target, 2)

            action = (
                f"add wildcard records or delegate to Cloud Load Balancer "
                f"({query_count_millions:.1f}M queries/mo -> ~{optimized_millions:.1f}M)"
            )
            recommendation = (
                recommendation_template
                or "Overprovisioned Cloud DNS — {action} (current: {current_sku})"
            ).format(current_sku=current_sku, target_sku=f"{optimized_millions}M queries/mo", action=action)

            details = {
                "recommendation": recommendation,
                "zone_name": current_sku,
                "zone_type": zone_type,
                "action": "optimize_query_volume",
                "query_count_millions_per_month": query_count_millions,
                "optimized_query_millions": optimized_millions,
                "monthly_cost_current": monthly_cost_current,
                "monthly_cost_target": monthly_cost_target,
                "monthly_savings": monthly_savings,
                "annual_savings": annual_savings,
                "target_skus": [{"label": f"{optimized_millions}M queries/mo", "monthly_cost": monthly_cost_target}],
                "is_fallback": False,
            }

            logger.info(
                f"[CLOUDDNS_OVERPROV] {current_sku}: {query_count_millions}M -> {optimized_millions}M queries/mo, "
                f"annual_savings=${annual_savings}"
            )
            return [recommendation, annual_cost_current, annual_cost_target, annual_savings, details]

        logger.info(f"[CLOUDDNS_OVERPROV] {current_sku}: no actionable overprovisioned signal")
        return None
