"""
GCP Cloud Storage Rightsizing Engine
======================================

Handles two recommendation types for Cloud Storage buckets:

  CLOUDSTORAGE_IDLE
    - No ReadObject, WriteObject, or ListObjects operations for 14-30 days
    - AND storage is flat (no new data ingested)
    - Recommendation: Apply lifecycle rule to transition to Nearline/Coldline/Archive,
      or delete empty bucket
    - Savings = difference between Standard and target storage class pricing

  CLOUDSTORAGE_OVERPROVISIONED
    - Data stored in Standard class but access frequency < once per month
      (api/request_count ReadObject P95 < 1 read/month)
    - OR storage/total_bytes growing > 10% per week with zero reads
      (data accumulating without being consumed)
    - Recommendation: Transition to Nearline, Coldline, or Archive based on access pattern

SKU catalog structure (cloudstorage_sku.json):
{
    "storage_classes": {
        "STANDARD":  { "price_per_gb_month": 0.020, "retrieval_per_gb": 0.0,   "min_duration_days": 0   },
        "NEARLINE":  { "price_per_gb_month": 0.010, "retrieval_per_gb": 0.01,  "min_duration_days": 30  },
        "COLDLINE":  { "price_per_gb_month": 0.004, "retrieval_per_gb": 0.02,  "min_duration_days": 90  },
        "ARCHIVE":   { "price_per_gb_month": 0.0012,"retrieval_per_gb": 0.05,  "min_duration_days": 365 }
    }
}

Billing model:
  - Storage: per GB-month (varies by class)
  - Operations: Class A (writes/lists) and Class B (reads) per 10k ops
  - Retrieval: per GB for Nearline/Coldline/Archive
  - Network egress: standard GCP egress rates
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

MONTHS_PER_YEAR = 12

# Storage class pricing (us-central1 / multi-region us)
STORAGE_CLASS_PRICING = {
    "STANDARD": {
        "price_per_gb_month": 0.020,
        "retrieval_per_gb": 0.0,
        "min_duration_days": 0,
        "label": "Standard",
    },
    "NEARLINE": {
        "price_per_gb_month": 0.010,
        "retrieval_per_gb": 0.01,
        "min_duration_days": 30,
        "label": "Nearline",
    },
    "COLDLINE": {
        "price_per_gb_month": 0.004,
        "retrieval_per_gb": 0.02,
        "min_duration_days": 90,
        "label": "Coldline",
    },
    "ARCHIVE": {
        "price_per_gb_month": 0.0012,
        "retrieval_per_gb": 0.05,
        "min_duration_days": 365,
        "label": "Archive",
    },
}

# Access frequency thresholds for class selection
# reads_per_month -> recommended class
ACCESS_FREQUENCY_RULES = [
    (1.0,   "NEARLINE"),   # < 1 read/month  -> Nearline
    (0.25,  "COLDLINE"),   # < 1 read/quarter -> Coldline
    (0.083, "ARCHIVE"),    # < 1 read/year   -> Archive
]


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _select_target_class(reads_per_month: float, current_class: str) -> Optional[str]:
    """Select the most cost-effective storage class based on access frequency."""
    for threshold, target_class in ACCESS_FREQUENCY_RULES:
        if reads_per_month < threshold:
            # Only recommend if it's actually a downgrade
            classes = list(STORAGE_CLASS_PRICING.keys())
            current_idx = classes.index(current_class) if current_class in classes else 0
            target_idx = classes.index(target_class)
            if target_idx > current_idx:
                return target_class
    return None


class CloudStorageEngine(BaseGCPEngine):
    """GCP Cloud Storage bucket rightsizing engine."""

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

        if rule_code == "CLOUDSTORAGE_IDLE":
            return self._handle_idle(current_sku, recommendation_template, metrics, resource_data)

        if rule_code == "CLOUDSTORAGE_OVERPROVISIONED":
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
        Idle: no reads/writes/lists for 14-30 days, storage flat.
        Recommendation: transition to Nearline or delete if empty.
        Savings = current Standard cost - Nearline cost (or full cost if empty).
        """
        storage_gb = _parse_num(resource_data.get("storage_gb", 0))
        current_class = (resource_data.get("storage_class") or "STANDARD").upper()
        current_pricing = STORAGE_CLASS_PRICING.get(current_class, STORAGE_CLASS_PRICING["STANDARD"])

        current_monthly = round(storage_gb * current_pricing["price_per_gb_month"], 4)
        current_annual = round(current_monthly * MONTHS_PER_YEAR, 2)

        if storage_gb == 0:
            # Empty bucket — recommend deletion
            recommendation = (
                recommendation_template
                or "Idle Cloud Storage bucket with zero objects — delete bucket (current: {current_sku})"
            ).format(current_sku=current_sku, target_sku="N/A")

            details = {
                "recommendation": recommendation,
                "current_class": current_class,
                "target_class": "DELETE",
                "storage_gb": 0,
                "current_monthly_cost": 0.0,
                "target_monthly_cost": 0.0,
                "monthly_savings": 0.0,
                "annual_savings": 0.0,
                "target_skus": [],
                "is_fallback": False,
            }
            return [recommendation, 0.0, 0.0, 0.0, details]

        # Non-empty idle bucket — transition to Nearline
        target_class = "NEARLINE"
        if current_class in ("NEARLINE", "COLDLINE", "ARCHIVE"):
            # Already in a cold class — recommend Coldline or Archive
            classes = list(STORAGE_CLASS_PRICING.keys())
            idx = classes.index(current_class)
            target_class = classes[min(idx + 1, len(classes) - 1)]

        target_pricing = STORAGE_CLASS_PRICING[target_class]
        target_monthly = round(storage_gb * target_pricing["price_per_gb_month"], 4)
        target_annual = round(target_monthly * MONTHS_PER_YEAR, 2)
        monthly_savings = round(current_monthly - target_monthly, 4)
        annual_savings = round(current_annual - target_annual, 2)

        recommendation = (
            recommendation_template
            or "Idle Cloud Storage bucket — transition from {current_sku} to {target_sku} storage class"
        ).format(current_sku=current_class, target_sku=target_class)

        details = {
            "recommendation": recommendation,
            "current_class": current_class,
            "target_class": target_class,
            "storage_gb": storage_gb,
            "current_monthly_cost": current_monthly,
            "target_monthly_cost": target_monthly,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
            "target_skus": [{"label": target_class, "monthly_cost": target_monthly}],
            "is_fallback": False,
        }

        logger.info(f"[CLOUDSTORAGE_IDLE] {current_sku}: {current_class} -> {target_class}, annual_savings=${annual_savings}")
        return [recommendation, current_annual, target_annual, annual_savings, details]

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
        Two overprovisioned signals per the documentation:

        Signal 1 — Low read frequency (primary):
          api/request_count (ReadObject) P95 < 1 read/month
          Select target class: Nearline (<1/mo), Coldline (<1/quarter), Archive (<1/year)

        Signal 2 — Growing storage with zero reads:
          storage/total_bytes growing > 10% per week AND read_ops = 0
          Data is accumulating without being consumed — transition to Archive.

        Retrieval fees are factored in to ensure the transition is net-positive.
        """
        storage_gb = _parse_num(resource_data.get("storage_gb", 0))
        current_class = (resource_data.get("storage_class") or "STANDARD").upper()
        reads_per_month = _parse_num(
            metrics.get("reads_per_month")
            or resource_data.get("reads_per_month", 0)
        )
        # storage/total_bytes growth signal: % growth per week
        storage_growth_pct_per_week = _parse_num(
            metrics.get("storage_growth_pct_per_week")
            or resource_data.get("storage_growth_pct_per_week", 0)
        )
        read_ops_30d = _parse_num(
            metrics.get("read_ops_30d")
            or resource_data.get("read_ops_30d", 0)
        )

        current_pricing = STORAGE_CLASS_PRICING.get(current_class, STORAGE_CLASS_PRICING["STANDARD"])
        current_monthly = round(storage_gb * current_pricing["price_per_gb_month"], 4)
        current_annual = round(current_monthly * MONTHS_PER_YEAR, 2)

        # Signal 2: growing storage with zero reads — force Archive
        growing_without_reads = (storage_growth_pct_per_week > 10.0 and read_ops_30d == 0)

        if growing_without_reads and current_class not in ("ARCHIVE",):
            target_class = "ARCHIVE"
            signal = "storage_growth_no_reads"
        else:
            # Signal 1: select class by read frequency
            target_class = _select_target_class(reads_per_month, current_class)
            signal = "low_read_frequency"

        if not target_class:
            logger.info(
                f"[CLOUDSTORAGE_OVERPROV] {current_sku}: reads/mo={reads_per_month}, "
                f"growth={storage_growth_pct_per_week}%/wk — no cheaper class available"
            )
            return None

        target_pricing = STORAGE_CLASS_PRICING[target_class]

        # Retrieval cost: reads_per_month * storage_gb * retrieval_per_gb
        # reads_per_month here is a count of read operations, not GB — use storage_gb as proxy
        # for data retrieved per read (conservative: assume 1 GB per read operation)
        retrieval_cost_monthly = round(
            reads_per_month * target_pricing["retrieval_per_gb"], 4
        )
        target_monthly = round(
            storage_gb * target_pricing["price_per_gb_month"] + retrieval_cost_monthly, 4
        )
        target_annual = round(target_monthly * MONTHS_PER_YEAR, 2)
        monthly_savings = round(current_monthly - target_monthly, 4)
        annual_savings = round(current_annual - target_annual, 2)

        if annual_savings <= 0:
            logger.info(
                f"[CLOUDSTORAGE_OVERPROV] {current_sku}: retrieval costs offset storage savings — no recommendation"
            )
            return None

        if signal == "storage_growth_no_reads":
            recommendation = (
                recommendation_template
                or "Overprovisioned Cloud Storage — transition from {current_sku} to {target_sku} "
                   "(storage growing {growth:.1f}%/wk with zero reads)"
            ).format(
                current_sku=current_class,
                target_sku=target_class,
                growth=storage_growth_pct_per_week,
            )
        else:
            recommendation = (
                recommendation_template
                or "Overprovisioned Cloud Storage — transition from {current_sku} to {target_sku} "
                   "(access < {reads_per_month:.2f} reads/mo)"
            ).format(
                current_sku=current_class,
                target_sku=target_class,
                reads_per_month=reads_per_month,
            )

        details = {
            "recommendation": recommendation,
            "signal": signal,
            "current_class": current_class,
            "target_class": target_class,
            "storage_gb": storage_gb,
            "reads_per_month": reads_per_month,
            "read_ops_30d": read_ops_30d,
            "storage_growth_pct_per_week": storage_growth_pct_per_week,
            "current_monthly_cost": current_monthly,
            "target_monthly_cost": target_monthly,
            "retrieval_cost_monthly": retrieval_cost_monthly,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
            "target_skus": [{"label": target_class, "monthly_cost": target_monthly}],
            "is_fallback": False,
        }

        logger.info(
            f"[CLOUDSTORAGE_OVERPROV] {current_sku}: signal={signal}, "
            f"{current_class} -> {target_class}, annual_savings=${annual_savings}"
        )
        return [recommendation, current_annual, target_annual, annual_savings, details]
