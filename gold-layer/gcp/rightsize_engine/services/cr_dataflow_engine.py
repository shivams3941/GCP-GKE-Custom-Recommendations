"""
GCP Dataflow Rightsizing Engine
=================================

Handles two recommendation types for Dataflow jobs:

  DATAFLOW_IDLE
    - element_count avg ≈ 0 AND current_num_vcpus avg > 0 over the window
    - Recommendation: Stop the job — workers are running but no data is flowing
    - Savings = full current worker cost

  DATAFLOW_OVERPROVISIONED
    - cpu_utilization P95 < 25% over the window
    - Recommendation: Reduce maxNumWorkers or switch to a smaller machine type
    - Finds the closest cheaper machine type in the same family

SKU catalog structure (cr_dataflow_sku.json):
{
    "<machine_family>": {           // e.g. "n1", "n2", "e2"
        "family": "n1",
        "region": "us-central1",
        "cpu_hourly_price": 0.031611,
        "ram_hourly_price_per_gb": 0.004237,
        "machine_types": [
            {
                "machine_type": "n1-standard-4",
                "family": "n1",
                "vcpus": 4,
                "ram_gb": 15,
                "hourly_price": 0.190,
                "monthly_price": 138.7,
                "annual_price": 1664.4,
                "ordinal": 5
            }
        ]
    }
}

Note: Dataflow worker pricing uses Compute Engine machine type rates.
The default Dataflow worker type is n1-standard-4 when not explicitly set.
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

HOURS_PER_MONTH = 730
HOURS_PER_YEAR = 8760

# Default worker type when job doesn't specify one
DEFAULT_WORKER_MACHINE_TYPE = "n1-standard-4"


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _extract_machine_family(machine_type: str) -> str:
    """Extract machine family prefix from a GCP machine type string."""
    if not machine_type:
        return ""
    parts = machine_type.split("-")
    if len(parts) >= 2:
        if len(parts[0]) <= 3 and parts[0][-1].isdigit():
            candidate = parts[0]
            if len(parts) > 1 and parts[1].isalpha() and len(parts[1]) <= 2:
                candidate = f"{parts[0]}{parts[1]}"
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


class DataflowEngine(BaseGCPEngine):
    """GCP Dataflow job rightsizing engine."""

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

        # Fall back to default worker type if not specified
        effective_sku = current_sku or DEFAULT_WORKER_MACHINE_TYPE

        if rule_code == "DATAFLOW_IDLE":
            return self._handle_idle(effective_sku, region, recommendation_template, resource_data)

        if rule_code == "DATAFLOW_OVERPROVISIONED":
            return self._handle_overprovisioned(
                effective_sku, region, recommendation_template, metrics, resource_data
            )

        logger.warning(f"Unknown rule_code: {rule_code}")
        return None

    # ------------------------------------------------------------------
    # IDLE handler — workers running but no data flowing
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
            logger.warning(f"[DATAFLOW_IDLE] Machine type '{current_sku}' not found in catalog")
            return None

        worker_count = _parse_num((resource_data or {}).get("current_num_vcpus", 1)) or 1
        current_specs = _extract_specs(current_entry)
        current_annual = round(current_specs["annual_cost"] * worker_count, 2)
        monthly = round(current_specs["monthly_cost"] * worker_count, 2)

        job_name = (resource_data or {}).get("job_name", "unknown")
        recommendation = (
            recommendation_template
            or "Idle Dataflow job — workers running with no data flowing, consider stopping (current: {current_sku})"
        ).format(current_sku=current_sku, target_sku="N/A")

        details = {
            "recommendation": recommendation,
            "job_name": job_name,
            "current_sku": current_specs,
            "worker_count": worker_count,
            "target_skus": [],
            "is_fallback": False,
            "monthly_savings": monthly,
            "annual_savings": current_annual,
        }

        logger.info(f"[DATAFLOW_IDLE] {current_sku} x{worker_count} workers -> annual_savings=${current_annual}")
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
            logger.warning(f"[DATAFLOW_OVERPROV] Machine type '{current_sku}' not found in catalog")
            return None

        family = current_entry.get("family") or _extract_machine_family(current_sku)
        family_entries = self._get_family_entries(family)
        if not family_entries:
            logger.warning(f"[DATAFLOW_OVERPROV] No catalog entries for family '{family}'")
            return None

        cur_vcpus = _parse_num(current_entry.get("vcpus"))
        cur_ram = _parse_num(current_entry.get("ram_gb"))
        cur_ordinal = current_entry.get("ordinal", 999)
        worker_count = _parse_num((resource_data or {}).get("current_num_vcpus", 1)) or 1

        if cur_vcpus == 0:
            logger.warning(f"[DATAFLOW_OVERPROV] No vCPU info for {current_sku}")
            return None

        cpu_util = _parse_num((metrics or {}).get("cpu_utilization_p95") or (metrics or {}).get("cpu_utilization_avg"))
        mem_util = _parse_num((metrics or {}).get("memory_utilization_avg"))

        # Required capacity with 1.3x safety margin
        required_vcpus = max(1, cur_vcpus * (cpu_util / 100) * 1.3) if cpu_util > 0 else cur_vcpus * 0.5
        required_ram = max(1, cur_ram * (mem_util / 100) * 1.3) if mem_util > 0 else cur_ram * 0.5

        logger.info(
            f"[DATAFLOW_OVERPROV] {current_sku}: vcpus={cur_vcpus}, ram={cur_ram}GB, "
            f"cpu_util={cpu_util}%, mem_util={mem_util}%, "
            f"required_vcpus={required_vcpus:.1f}, required_ram={required_ram:.1f}GB"
        )

        candidates = [
            e for e in family_entries
            if e.get("machine_type") != current_sku
            and e.get("ordinal", 999) < cur_ordinal
            and _parse_num(e.get("vcpus")) >= required_vcpus
            and _parse_num(e.get("ram_gb")) >= required_ram
        ]

        if not candidates:
            logger.info(f"[DATAFLOW_OVERPROV] No direct candidate for {current_sku}, trying fallback")
            return self._fallback_closest_cheaper(
                current_entry, family_entries, family, worker_count, recommendation_template
            )

        candidates.sort(key=lambda x: (_parse_num(x.get("vcpus")), _parse_num(x.get("ram_gb")), x.get("ordinal", 999)))
        best = candidates[0]

        return self._build_response(
            current_entry, [best], family, worker_count,
            is_fallback=False, recommendation_template=recommendation_template
        )

    def _fallback_closest_cheaper(
        self,
        current_entry: Dict,
        family_entries: List[Dict],
        family: str,
        worker_count: float,
        recommendation_template: str,
    ) -> Optional[List]:
        cur_ordinal = current_entry.get("ordinal", 999)
        cur_machine = current_entry.get("machine_type", "")

        cheaper = [
            e for e in family_entries
            if e.get("machine_type") != cur_machine and e.get("ordinal", 999) < cur_ordinal
        ]

        if not cheaper:
            logger.info(f"[DATAFLOW_FALLBACK] No cheaper machine type found for {cur_machine}")
            return None

        cheaper.sort(key=lambda x: -x.get("ordinal", 0))
        top3 = cheaper[:3]

        return self._build_response(
            current_entry, top3, family, worker_count,
            is_fallback=True, recommendation_template=recommendation_template
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_machine_type_entry(self, machine_type: str) -> Optional[Dict]:
        # Try batch first, then streaming
        for job_type_key in ("batch", "streaming"):
            section = self.sku_catalog.get(job_type_key, {})
            for entry in section.get("machine_types", []):
                if entry.get("machine_type") == machine_type:
                    return entry
        return None

    def _get_family_entries(self, family: str) -> List[Dict]:
        # Dataflow SKU is flat (not family-keyed) — return all batch machine types
        return self.sku_catalog.get("batch", {}).get("machine_types", [])

    def _build_response(
        self,
        current_entry: Dict,
        target_entries: List[Dict],
        family: str,
        worker_count: float,
        is_fallback: bool,
        recommendation_template: str,
    ) -> List:
        current_specs = _extract_specs(current_entry)
        target_specs = [_extract_specs(e) for e in target_entries]
        count = len(target_specs)

        avg_hourly = round(sum(t["hourly_cost"] for t in target_specs) / count, 6)
        avg_monthly = round(sum(t["monthly_cost"] for t in target_specs) / count, 4)
        avg_annual = round(sum(t["annual_cost"] for t in target_specs) / count, 4)

        current_annual_total = round(current_specs["annual_cost"] * worker_count, 2)
        target_annual_total = round(avg_annual * worker_count, 2)
        monthly_savings = round((current_specs["monthly_cost"] - avg_monthly) * worker_count, 2)
        annual_savings = round(current_annual_total - target_annual_total, 2)

        target_names = ", ".join(t["machine_type"] for t in target_specs)
        recommendation = (recommendation_template or "Rightsize from {current_sku} to {target_sku}").format(
            current_sku=current_specs["machine_type"],
            target_sku=target_names,
        )

        logger.info(
            f"[DATAFLOW_ENGINE] {'Fallback' if is_fallback else 'Direct'}: "
            f"{current_specs['machine_type']} -> {target_names} x{worker_count} workers, "
            f"annual_savings=${annual_savings}"
        )

        details = {
            "recommendation": recommendation,
            "family": family,
            "worker_count": worker_count,
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
