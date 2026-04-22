"""
GCP Cloud Router Rightsizing Engine
=====================================

Handles two recommendation types for Cloud Routers:

  CLOUDROUTER_IDLE
    - BGP session_up avg = 0 AND nat/allocated_ports avg = 0 over the window
    - Recommendation: Delete the router — no active BGP sessions or NAT usage
    - Savings = full current cost (Cloud Router charges per VPN tunnel/Interconnect hour)

  CLOUDROUTER_NAT_OVERPROVISIONED
    - NAT port usage P95 < 30% of allocated ports over the window
    - Recommendation: Reduce min_ports_per_vm or reduce NAT IP count
    - Savings estimated from excess NAT IP cost

SKU catalog structure (cr_cloudrouter_sku.json):
{
    "nat_ip": {
        "region": "us-central1",
        "hourly_price": 0.004,          // per NAT IP per hour
        "monthly_price": 2.92,
        "annual_price": 35.04
    },
    "vpn_tunnel": {
        "region": "us-central1",
        "hourly_price": 0.05,           // per VPN tunnel per hour
        "monthly_price": 36.5,
        "annual_price": 438.0
    },
    "interconnect_vlan": {
        "region": "us-central1",
        "hourly_price": 0.0,            // billed by Interconnect attachment, not router
        "monthly_price": 0.0,
        "annual_price": 0.0
    }
}
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

HOURS_PER_MONTH = 730
HOURS_PER_YEAR = 8760


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


class CloudRouterEngine(BaseGCPEngine):
    """GCP Cloud Router rightsizing engine."""

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
        if not self.sku_catalog:
            logger.warning("SKU catalog is empty")
            return None

        if rule_code == "CLOUDROUTER_IDLE":
            return self._handle_idle(region, recommendation_template, resource_data)

        if rule_code == "CLOUDROUTER_NAT_OVERPROVISIONED":
            return self._handle_nat_overprovisioned(
                region, recommendation_template, metrics, resource_data
            )

        logger.warning(f"Unknown rule_code: {rule_code}")
        return None

    # ------------------------------------------------------------------
    # IDLE handler — router has no active BGP sessions or NAT usage
    # ------------------------------------------------------------------

    def _handle_idle(
        self,
        region: str,
        recommendation_template: str,
        resource_data: Dict,
    ) -> Optional[List]:
        vpn_entry = self.sku_catalog.get("vpn_tunnel", {})
        if not vpn_entry:
            logger.warning("[CLOUDROUTER_IDLE] No vpn_tunnel entry in SKU catalog")
            return None

        tunnel_count = _parse_num((resource_data or {}).get("vpn_tunnel_count", 1)) or 1
        hourly = _parse_num(vpn_entry.get("hourly_price", 0.05))
        current_annual = round(hourly * HOURS_PER_YEAR * tunnel_count, 2)
        monthly = round(hourly * HOURS_PER_MONTH * tunnel_count, 2)

        router_name = (resource_data or {}).get("router_name", "unknown")
        recommendation = (
            recommendation_template
            or "Idle Cloud Router — no active BGP sessions or NAT usage, consider deleting (router: {current_sku})"
        ).format(current_sku=router_name, target_sku="N/A")

        details = {
            "recommendation": recommendation,
            "router_name": router_name,
            "tunnel_count": tunnel_count,
            "current_sku": {"hourly_price": hourly, "annual_cost": current_annual},
            "target_skus": [],
            "is_fallback": False,
            "monthly_savings": monthly,
            "annual_savings": current_annual,
        }

        logger.info(f"[CLOUDROUTER_IDLE] {router_name} x{tunnel_count} tunnels -> annual_savings=${current_annual}")
        return [recommendation, current_annual, 0.0, current_annual, details]

    # ------------------------------------------------------------------
    # NAT OVERPROVISIONED handler — reduce NAT IPs or min_ports_per_vm
    # ------------------------------------------------------------------

    def _handle_nat_overprovisioned(
        self,
        region: str,
        recommendation_template: str,
        metrics: Dict[str, float],
        resource_data: Dict,
    ) -> Optional[List]:
        nat_entry = self.sku_catalog.get("nat_ip", {})
        if not nat_entry:
            logger.warning("[CLOUDROUTER_NAT_OVERPROV] No nat_ip entry in SKU catalog")
            return None

        nat_ip_count = _parse_num((resource_data or {}).get("nat_ip_count", 1)) or 1
        port_usage_p95 = _parse_num((metrics or {}).get("nat_port_usage_p95"))
        allocated_ports = _parse_num((metrics or {}).get("nat_allocated_ports_avg"))

        hourly_per_ip = _parse_num(nat_entry.get("hourly_price", 0.004))
        current_annual = round(hourly_per_ip * HOURS_PER_YEAR * nat_ip_count, 2)

        # Estimate how many IPs are actually needed based on P95 port usage
        if allocated_ports > 0 and port_usage_p95 > 0:
            utilization_ratio = port_usage_p95 / allocated_ports
            required_ips = max(1, round(nat_ip_count * utilization_ratio * 1.2))  # 1.2x safety margin
        else:
            required_ips = max(1, round(nat_ip_count * 0.5))

        if required_ips >= nat_ip_count:
            logger.info(f"[CLOUDROUTER_NAT_OVERPROV] Required IPs ({required_ips}) >= current ({nat_ip_count}), no savings")
            return None

        target_annual = round(hourly_per_ip * HOURS_PER_YEAR * required_ips, 2)
        annual_savings = round(current_annual - target_annual, 2)
        monthly_savings = round((current_annual - target_annual) / 12, 2)

        router_name = (resource_data or {}).get("router_name", "unknown")
        recommendation = (
            recommendation_template
            or "Overprovisioned Cloud NAT — reduce NAT IPs from {current_sku} to {target_sku}"
        ).format(current_sku=str(nat_ip_count), target_sku=str(required_ips))

        details = {
            "recommendation": recommendation,
            "router_name": router_name,
            "current_nat_ip_count": nat_ip_count,
            "recommended_nat_ip_count": required_ips,
            "port_usage_p95": port_usage_p95,
            "allocated_ports_avg": allocated_ports,
            "current_sku": {"nat_ip_count": nat_ip_count, "annual_cost": current_annual},
            "target_skus": [{"nat_ip_count": required_ips, "annual_cost": target_annual}],
            "is_fallback": False,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
        }

        logger.info(
            f"[CLOUDROUTER_NAT_OVERPROV] {router_name}: {nat_ip_count} IPs -> {required_ips} IPs, "
            f"annual_savings=${annual_savings}"
        )
        return [recommendation, current_annual, target_annual, annual_savings, details]
