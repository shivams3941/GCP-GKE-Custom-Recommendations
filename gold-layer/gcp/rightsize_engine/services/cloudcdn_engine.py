"""
GCP Cloud CDN Rightsizing Engine
===================================

Handles two recommendation types for Cloud CDN origins (backend services):

  CLOUDCDN_IDLE
    - Zero requests hitting CDN over 14-30 days
    - OR zero edge bytes served (loadbalancing.googleapis.com/https/sent_bytes_count ≈ 0)
    - OR cache_hit_ratio = 0% consistently (serving.googleapis.com/cdn/cache_hit_ratio
      or cloud_load_balancing.googleapis.com/edge/cache_hit_ratio = 0%)
    - Each is an independent idle signal per the documentation
    - Recommendation: Delete CDN origin or disable CDN on backend service
    - Savings = edge egress cost eliminated

  CLOUDCDN_OVERPROVISIONED
    - Cache hit ratio < 30% (CDN is ineffective — paying CDN premium for origin traffic)
    - OR high cache miss rate (> 20% requests hitting origin)
    - OR backend_latencies P95 > 500ms on misses (slow origin)
    - Recommendation: Optimize TTL/cache mode, force aggressive caching,
      or tune cache keys / origin performance

SKU catalog structure (cloudcdn_sku.json):
{
    "pricing": {
        "egress_price_per_gb_na": 0.08,
        "egress_price_per_gb_eu": 0.08,
        "egress_price_per_gb_apac": 0.14,
        "cache_invalidation_per_10k_paths": 0.012
    }
}

Billing model:
  - Edge egress: per GB served from CDN edge (varies by region)
  - Cache invalidations: $0.012 per 10k paths
  - No charge for cache hits vs misses distinction — all egress billed equally
  - Cache misses = origin egress + origin compute cost (additional cost)
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

MONTHS_PER_YEAR = 12

# Cloud CDN egress pricing (per GB)
EGRESS_PRICE_PER_GB = {
    "NA":   0.08,   # North America
    "EU":   0.08,   # Europe
    "APAC": 0.14,   # Asia Pacific
    "SA":   0.12,   # South America
    "AU":   0.19,   # Australia
}
DEFAULT_EGRESS_PRICE = 0.08  # fallback

# Cache invalidation pricing
INVALIDATION_PRICE_PER_10K = 0.012


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _get_egress_price(region: str) -> float:
    """Map GCP region to CDN egress price per GB."""
    region_upper = (region or "").upper()
    if region_upper.startswith("US") or region_upper.startswith("NORTHAMERICA"):
        return EGRESS_PRICE_PER_GB["NA"]
    if region_upper.startswith("EUROPE"):
        return EGRESS_PRICE_PER_GB["EU"]
    if region_upper.startswith("ASIA") or region_upper.startswith("SOUTHASIA"):
        return EGRESS_PRICE_PER_GB["APAC"]
    if region_upper.startswith("SOUTHAMERICA"):
        return EGRESS_PRICE_PER_GB["SA"]
    if region_upper.startswith("AUSTRALIA"):
        return EGRESS_PRICE_PER_GB["AU"]
    return DEFAULT_EGRESS_PRICE


class CloudCDNEngine(BaseGCPEngine):
    """GCP Cloud CDN origin rightsizing engine."""

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

        if rule_code == "CLOUDCDN_IDLE":
            return self._handle_idle(current_sku, region, recommendation_template, metrics, resource_data)

        if rule_code == "CLOUDCDN_OVERPROVISIONED":
            return self._handle_overprovisioned(current_sku, region, recommendation_template, metrics, resource_data)

        logger.warning(f"Unknown rule_code: {rule_code}")
        return None

    # ------------------------------------------------------------------
    # IDLE handler
    # ------------------------------------------------------------------

    def _handle_idle(
        self,
        current_sku: str,
        region: str,
        recommendation_template: str,
        metrics: Dict[str, float],
        resource_data: Dict,
    ) -> Optional[List]:
        """
        Three independent idle signals per the documentation:

          1. loadbalancing.googleapis.com/https/request_count ≈ 0 over 14-30 days
          2. loadbalancing.googleapis.com/https/sent_bytes_count ≈ 0 bytes over 14 days
          3. serving.googleapis.com/cdn/cache_hit_ratio = 0% consistently over 30 days
             (or cloud_load_balancing.googleapis.com/edge/cache_hit_ratio = 0%)

        Any one of these independently signals an idle/unused CDN origin.
        Savings = edge egress cost that would have been incurred.
        """
        egress_gb_monthly = _parse_num(
            metrics.get("egress_gb_monthly") or resource_data.get("egress_gb_monthly", 0)
        )
        cache_hit_ratio = _parse_num(metrics.get("cache_hit_ratio", 0))
        request_count = _parse_num(metrics.get("request_count_monthly", 0))
        # sent_bytes_gb is the direct field from the fetch catalog
        sent_bytes_gb = _parse_num(
            metrics.get("sent_bytes_gb")
            or resource_data.get("sent_bytes_gb", egress_gb_monthly)
        )
        # edge_cache_hit_ratio from cloud_load_balancing.googleapis.com/edge/cache_hit_ratio
        edge_cache_hit_ratio = _parse_num(
            metrics.get("edge_cache_hit_ratio")
            or resource_data.get("edge_cache_hit_ratio", cache_hit_ratio)
        )

        egress_price = _get_egress_price(region)
        monthly_cost = round(egress_gb_monthly * egress_price, 4)
        annual_cost = round(monthly_cost * MONTHS_PER_YEAR, 2)

        # Determine which signal(s) triggered — each is independent per the documentation
        triggered_signals = []
        if request_count == 0:
            triggered_signals.append("zero requests (14-30 days)")
        if sent_bytes_gb == 0:
            triggered_signals.append("zero edge bytes served (sent_bytes_count = 0)")
        if cache_hit_ratio == 0 or edge_cache_hit_ratio == 0:
            triggered_signals.append("cache never used (cache_hit_ratio = 0%)")

        if not triggered_signals:
            # Fallback: low overall traffic
            triggered_signals.append("no meaningful CDN traffic over 14-30 days")

        reason = "; ".join(triggered_signals)

        recommendation = (
            recommendation_template
            or "Idle Cloud CDN origin — disable CDN or delete backend service {current_sku} ({reason})"
        ).format(current_sku=current_sku, target_sku="N/A", reason=reason)

        details = {
            "recommendation": recommendation,
            "origin_name": current_sku,
            "region": region,
            "egress_gb_monthly": egress_gb_monthly,
            "sent_bytes_gb": sent_bytes_gb,
            "cache_hit_ratio": cache_hit_ratio,
            "edge_cache_hit_ratio": edge_cache_hit_ratio,
            "request_count_monthly": request_count,
            "triggered_signals": triggered_signals,
            "egress_price_per_gb": egress_price,
            "monthly_cost": monthly_cost,
            "annual_cost": annual_cost,
            "monthly_savings": monthly_cost,
            "annual_savings": annual_cost,
            "reason": reason,
            "target_skus": [],
            "is_fallback": False,
        }

        logger.info(f"[CLOUDCDN_IDLE] {current_sku}: signals={triggered_signals}, annual_savings=${annual_cost}")
        return [recommendation, annual_cost, 0.0, annual_cost, details]

    # ------------------------------------------------------------------
    # OVERPROVISIONED handler
    # ------------------------------------------------------------------

    def _handle_overprovisioned(
        self,
        current_sku: str,
        region: str,
        recommendation_template: str,
        metrics: Dict[str, float],
        resource_data: Dict,
    ) -> Optional[List]:
        """
        Overprovisioned signals per the documentation:

          1. cache_hit_ratio < 30% — CDN is ineffective, paying CDN premium for origin traffic
          2. backend_request_count MISS > 20% of total requests — high origin load
          3. backend_latencies P95 > 500ms on misses — slow origin causing poor CDN performance

        Savings = cost reduction from improving cache hit ratio.
        A cache hit ratio < 30% means most traffic hits the origin,
        incurring both CDN egress AND origin compute/egress costs.
        Improving to > 80% hit ratio eliminates ~70% of origin traffic.
        """
        egress_gb_monthly = _parse_num(metrics.get("egress_gb_monthly") or resource_data.get("egress_gb_monthly", 0))
        cache_hit_ratio = _parse_num(metrics.get("cache_hit_ratio", 0))
        miss_ratio_pct = _parse_num(metrics.get("miss_ratio_pct", 0))
        backend_latency_p95_ms = _parse_num(
            metrics.get("backend_latency_p95_ms")
            or resource_data.get("backend_latency_p95_ms", 0)
        )
        cache_mode = resource_data.get("cache_mode", "CACHE_ALL_STATIC")

        egress_price = _get_egress_price(region)

        # Current cost: CDN egress for all traffic
        monthly_cost_current = round(egress_gb_monthly * egress_price, 4)
        annual_cost_current = round(monthly_cost_current * MONTHS_PER_YEAR, 2)

        # Target: improve cache hit ratio to 80% (static) or 60% (dynamic)
        is_dynamic = "DYNAMIC" in (cache_mode or "").upper()
        target_hit_ratio = 60.0 if is_dynamic else 80.0
        current_hit_ratio = cache_hit_ratio

        # Collect all fired signals
        signals_fired = []
        if current_hit_ratio < 30:
            signals_fired.append(
                f"cache_hit_ratio={current_hit_ratio:.0f}% < 30% — CDN is ineffective"
            )
        elif current_hit_ratio < target_hit_ratio:
            signals_fired.append(
                f"cache_hit_ratio={current_hit_ratio:.0f}% below target {target_hit_ratio:.0f}%"
            )
        if miss_ratio_pct > 20:
            signals_fired.append(
                f"miss_ratio={miss_ratio_pct:.0f}% > 20% — high origin load"
            )
        if backend_latency_p95_ms > 500:
            signals_fired.append(
                f"backend_latency P95={backend_latency_p95_ms:.0f}ms > 500ms — slow origin"
            )

        if current_hit_ratio >= target_hit_ratio and miss_ratio_pct <= 20 and backend_latency_p95_ms <= 500:
            logger.info(
                f"[CLOUDCDN_OVERPROV] {current_sku}: all signals within acceptable range — no recommendation"
            )
            return None

        # Savings from reducing origin requests (miss traffic)
        hit_improvement = max(0.0, (target_hit_ratio - current_hit_ratio) / 100.0)
        origin_traffic_reduction_gb = round(egress_gb_monthly * hit_improvement, 4)
        origin_savings_monthly = round(origin_traffic_reduction_gb * egress_price, 4)
        monthly_cost_target = round(monthly_cost_current - origin_savings_monthly, 4)
        annual_cost_target = round(monthly_cost_target * MONTHS_PER_YEAR, 2)
        monthly_savings = origin_savings_monthly
        annual_savings = round(monthly_savings * MONTHS_PER_YEAR, 2)

        # Build action based on which signals fired
        actions = []
        if current_hit_ratio < 30:
            actions.append(
                f"force aggressive caching (cache_hit_ratio={current_hit_ratio:.0f}% < 30% — "
                f"paying CDN premium for origin traffic)"
            )
        elif current_hit_ratio < target_hit_ratio:
            actions.append(
                f"optimize TTL and cache keys to improve hit ratio "
                f"from {current_hit_ratio:.0f}% to {target_hit_ratio:.0f}%"
            )
        if miss_ratio_pct > 20:
            actions.append(
                f"reduce cache misses (current miss rate {miss_ratio_pct:.0f}% > 20%)"
            )
        if backend_latency_p95_ms > 500:
            actions.append(
                f"tune origin TTL/cache mode to reduce origin latency "
                f"(P95={backend_latency_p95_ms:.0f}ms > 500ms)"
            )

        action = "; ".join(actions) if actions else "optimize cache configuration"

        recommendation = (
            recommendation_template
            or "Overprovisioned Cloud CDN — {action} (current: {current_sku})"
        ).format(
            current_sku=current_sku,
            target_sku=f"{target_hit_ratio:.0f}% hit ratio",
            action=action,
        )

        details = {
            "recommendation": recommendation,
            "origin_name": current_sku,
            "region": region,
            "cache_mode": cache_mode,
            "cache_hit_ratio_current": cache_hit_ratio,
            "cache_hit_ratio_target": target_hit_ratio,
            "miss_ratio_pct": miss_ratio_pct,
            "backend_latency_p95_ms": backend_latency_p95_ms,
            "egress_gb_monthly": egress_gb_monthly,
            "origin_traffic_reduction_gb": origin_traffic_reduction_gb,
            "egress_price_per_gb": egress_price,
            "monthly_cost_current": monthly_cost_current,
            "monthly_cost_target": monthly_cost_target,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
            "signals_fired": signals_fired,
            "action": action,
            "target_skus": [{"label": f"{target_hit_ratio:.0f}% hit ratio", "monthly_cost": monthly_cost_target}],
            "is_fallback": False,
        }

        logger.info(
            f"[CLOUDCDN_OVERPROV] {current_sku}: signals={signals_fired}, "
            f"hit_ratio={cache_hit_ratio}% -> {target_hit_ratio}%, annual_savings=${annual_savings}"
        )
        return [recommendation, annual_cost_current, annual_cost_target, annual_savings, details]
