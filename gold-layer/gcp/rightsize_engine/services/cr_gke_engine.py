"""
GCP GKE Rightsizing Engine
===========================

Handles two recommendation types for GKE node pools:

  GKE_IDLE
    - Node pool CPU avg < 5% AND memory avg < 20% over the window
    - Recommendation: Delete / scale down to 0 nodes
    - No target SKU needed — savings = full current cost

  GKE_OVERPROVISIONED
    - Node pool CPU avg < 40% OR memory avg < 50%
    - Recommendation: Downsize to a smaller machine type in the same family
    - Finds the closest cheaper machine type that still fits the workload

SKU catalog structure (cr_gke_sku.json):
{
    "<machine_family>": {           // e.g. "e2", "n1", "n2"
        "family": "e2",
        "region": "us-central1",
        "cpu_hourly_price": 0.02289,
        "ram_hourly_price_per_gb": 0.003067,
        "machine_types": [
            {
                "machine_type": "e2-standard-4",
                "family": "e2",
                "vcpus": 4,
                "ram_gb": 16,
                "hourly_price": 0.134632,
                "monthly_price": 98.28,
                "annual_price": 1179.3,
                "ordinal": 7          // lower = cheaper
            }
        ]
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


def _extract_machine_family(machine_type: str) -> str:
    """Extract machine family prefix from a GCP machine type string.

    Examples:
        e2-standard-4   -> e2
        n2-highmem-8    -> n2
        n2d-standard-16 -> n2d
        c2-standard-8   -> c2
        c2d-standard-4  -> c2d
        t2d-standard-2  -> t2d
    """
    if not machine_type:
        return ""
    parts = machine_type.split("-")
    if len(parts) >= 2:
        # Handle families like n2d, c2d, t2d (prefix has digit + letter)
        if len(parts[0]) <= 3 and parts[0][-1].isdigit():
            # Could be n2d, c2d — check if second part is also a prefix
            candidate = parts[0]
            if len(parts) > 1 and parts[1].isalpha() and len(parts[1]) <= 2:
                candidate = f"{parts[0]}{parts[1]}"
                # Validate it looks like a known compound family
                if candidate in ("n2d", "c2d", "t2d", "t2a", "m1", "m2", "a2"):
                    return candidate
        return parts[0]
    return machine_type


def _extract_specs(entry: Dict) -> Dict:
    """Normalize a machine_type catalog entry into a standard specs dict."""
    hourly = _parse_num(entry.get("hourly_price"))
    return {
        "machine_type": entry.get("machine_type", ""),
        "family": entry.get("family", ""),
        "vcpus": _parse_num(entry.get("vcpus")),
        "ram_gb": _parse_num(entry.get("ram_gb")),
        "hourly_cost": hourly,
        "monthly_cost": round(hourly * HOURS_PER_MONTH, 4),
        "annual_cost": round(hourly * HOURS_PER_YEAR, 4),
        "ordinal": entry.get("ordinal", 999),
    }


class GKEEngine(BaseGCPEngine):
    """GCP GKE node pool rightsizing engine."""

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

        if rule_code == "GKE_IDLE":
            return self._handle_idle(current_sku, region, recommendation_template, resource_data)

        if rule_code == "GKE_OVERPROVISIONED":
            return self._handle_overprovisioned(
                current_sku, region, recommendation_template, metrics, resource_data
            )

        logger.warning(f"Unknown rule_code: {rule_code}")
        return None

    # ------------------------------------------------------------------
    # IDLE handler — no target SKU, savings = full current cost
    # ------------------------------------------------------------------

    def _handle_idle(
        self,
        current_sku: str,
        region: str,
        recommendation_template: str,
        resource_data: Dict,
    ) -> Optional[List]:
        current_entry = self._find_machine_type_entry(current_sku)
        if not current_entry:
            logger.warning(f"[GKE_IDLE] Machine type '{current_sku}' not found in catalog")
            return None

        node_count = _parse_num((resource_data or {}).get("current_node_count", 1)) or 1
        current_specs = _extract_specs(current_entry)
        current_annual = round(current_specs["annual_cost"] * node_count, 2)

        recommendation = (recommendation_template or "Idle GKE node pool — consider deleting or scaling to 0").format(
            current_sku=current_sku, target_sku="N/A"
        )

        details = {
            "recommendation": recommendation,
            "family": current_specs["family"],
            "current_sku": current_specs,
            "node_count": node_count,
            "target_skus": [],
            "is_fallback": False,
            "avg_hourly_cost": 0.0,
            "avg_monthly_cost": 0.0,
            "avg_annual_cost": 0.0,
            "monthly_savings": round(current_specs["monthly_cost"] * node_count, 2),
            "annual_savings": current_annual,
        }

        logger.info(f"[GKE_IDLE] {current_sku} x{node_count} -> annual_savings=${current_annual}")
        return [recommendation, current_annual, 0.0, current_annual, details]

    # ------------------------------------------------------------------
    # OVERPROVISIONED handler — find smaller machine type in same family
    # ------------------------------------------------------------------

    def _handle_overprovisioned(
        self,
        current_sku: str,
        region: str,
        recommendation_template: str,
        metrics: Dict[str, float],
        resource_data: Dict,
    ) -> Optional[List]:
        current_entry = self._find_machine_type_entry(current_sku)
        if not current_entry:
            logger.warning(f"[GKE_OVERPROV] Machine type '{current_sku}' not found in catalog")
            return None

        family = current_entry.get("family") or _extract_machine_family(current_sku)
        family_entries = self._get_family_entries(family)
        if not family_entries:
            logger.warning(f"[GKE_OVERPROV] No catalog entries for family '{family}'")
            return None

        cur_vcpus = _parse_num(current_entry.get("vcpus"))
        cur_ram = _parse_num(current_entry.get("ram_gb"))
        cur_ordinal = current_entry.get("ordinal", 999)
        node_count = _parse_num((resource_data or {}).get("current_node_count", 1)) or 1

        if cur_vcpus == 0:
            logger.warning(f"[GKE_OVERPROV] No vCPU info for {current_sku}")
            return None

        # Determine required capacity from metrics
        cpu_util = _parse_num((metrics or {}).get("cpu_utilization_avg") or (metrics or {}).get("cpu_percent"))
        mem_util = _parse_num((metrics or {}).get("memory_utilization_avg") or (metrics or {}).get("memory_percent"))

        # Required vCPUs = current * utilization * safety_margin (1.3)
        required_vcpus = max(1, cur_vcpus * (cpu_util / 100) * 1.3) if cpu_util > 0 else cur_vcpus * 0.5
        required_ram = max(1, cur_ram * (mem_util / 100) * 1.3) if mem_util > 0 else cur_ram * 0.5

        logger.info(
            f"[GKE_OVERPROV] {current_sku}: vcpus={cur_vcpus}, ram={cur_ram}GB, "
            f"cpu_util={cpu_util}%, mem_util={mem_util}%, "
            f"required_vcpus={required_vcpus:.1f}, required_ram={required_ram:.1f}GB"
        )

        # Find candidates: cheaper, fits required capacity
        candidates = []
        for entry in family_entries:
            if entry.get("machine_type") == current_sku:
                continue
            if entry.get("ordinal", 999) >= cur_ordinal:
                continue  # must be cheaper

            tgt_vcpus = _parse_num(entry.get("vcpus"))
            tgt_ram = _parse_num(entry.get("ram_gb"))

            if tgt_vcpus < required_vcpus:
                continue
            if tgt_ram < required_ram:
                continue

            candidates.append(entry)

        if not candidates:
            logger.info(f"[GKE_OVERPROV] No direct candidate for {current_sku}, trying fallback")
            return self._fallback_closest_cheaper(
                current_entry, family_entries, family, node_count, recommendation_template
            )

        # Pick closest match: fewest vCPUs that still fit, then cheapest
        candidates.sort(key=lambda x: (_parse_num(x.get("vcpus")), _parse_num(x.get("ram_gb")), x.get("ordinal", 999)))
        best = candidates[0]

        return self._build_response(
            current_entry, [best], family, node_count,
            is_fallback=False, recommendation_template=recommendation_template
        )

    def _fallback_closest_cheaper(
        self,
        current_entry: Dict,
        family_entries: List[Dict],
        family: str,
        node_count: float,
        recommendation_template: str,
    ) -> Optional[List]:
        """Return up to 3 closest cheaper machine types in the same family."""
        cur_ordinal = current_entry.get("ordinal", 999)
        cur_machine = current_entry.get("machine_type", "")

        cheaper = [
            e for e in family_entries
            if e.get("machine_type") != cur_machine and e.get("ordinal", 999) < cur_ordinal
        ]

        if not cheaper:
            logger.info(f"[GKE_FALLBACK] No cheaper machine type found for {cur_machine}")
            return None

        # Sort by closest ordinal to current (highest ordinal first = closest in price)
        cheaper.sort(key=lambda x: -x.get("ordinal", 0))
        top3 = cheaper[:3]

        return self._build_response(
            current_entry, top3, family, node_count,
            is_fallback=True, recommendation_template=recommendation_template
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_machine_type_entry(self, machine_type: str) -> Optional[Dict]:
        """Look up a machine type entry across all families in the catalog."""
        family = _extract_machine_family(machine_type)
        family_data = self.sku_catalog.get(family, {})
        for entry in family_data.get("machine_types", []):
            if entry.get("machine_type") == machine_type:
                return entry

        # Fallback: search all families
        for fam_data in self.sku_catalog.values():
            for entry in fam_data.get("machine_types", []):
                if entry.get("machine_type") == machine_type:
                    return entry

        return None

    def _get_family_entries(self, family: str) -> List[Dict]:
        """Return all machine_type entries for a given family."""
        return self.sku_catalog.get(family, {}).get("machine_types", [])

    def _build_response(
        self,
        current_entry: Dict,
        target_entries: List[Dict],
        family: str,
        node_count: float,
        is_fallback: bool,
        recommendation_template: str,
    ) -> List:
        current_specs = _extract_specs(current_entry)
        target_specs = [_extract_specs(e) for e in target_entries]
        count = len(target_specs)

        avg_hourly = round(sum(t["hourly_cost"] for t in target_specs) / count, 6)
        avg_monthly = round(sum(t["monthly_cost"] for t in target_specs) / count, 4)
        avg_annual = round(sum(t["annual_cost"] for t in target_specs) / count, 4)

        # Multiply by node_count for cluster-level cost
        current_annual_total = round(current_specs["annual_cost"] * node_count, 2)
        target_annual_total = round(avg_annual * node_count, 2)
        monthly_savings = round((current_specs["monthly_cost"] - avg_monthly) * node_count, 2)
        annual_savings = round(current_annual_total - target_annual_total, 2)

        target_names = ", ".join(t["machine_type"] for t in target_specs)
        recommendation = (recommendation_template or "Rightsize from {current_sku} to {target_sku}").format(
            current_sku=current_specs["machine_type"],
            target_sku=target_names,
        )

        logger.info(
            f"[GKE_ENGINE] {'Fallback' if is_fallback else 'Direct'}: "
            f"{current_specs['machine_type']} -> {target_names} x{node_count} nodes, "
            f"annual_savings=${annual_savings}"
        )

        details = {
            "recommendation": recommendation,
            "family": family,
            "node_count": node_count,
            "current_sku": current_specs,
            "target_skus": target_specs,
            "is_fallback": is_fallback,
            "avg_hourly_cost": avg_hourly,
            "avg_monthly_cost": avg_monthly,
            "avg_annual_cost": avg_annual,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
        }

        return [recommendation, current_annual_total, target_annual_total, annual_savings, details]
