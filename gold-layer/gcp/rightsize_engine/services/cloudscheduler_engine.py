"""
GCP Cloud Scheduler Rightsizing Engine
========================================

Handles two recommendation types for Cloud Scheduler jobs:

  CLOUDSCHEDULER_IDLE
    - Job has zero successful attempts over 14-30 days
    - OR job is paused (paused jobs still incur monthly billing)
    - OR job targets a deleted/unreachable resource (100% error rate)
    - Recommendation: Delete job
    - Savings = per-job monthly fee * 12

  CLOUDSCHEDULER_OVERPROVISIONED
    - Job executes very frequently (e.g. every 1 min) for low-value work
    - OR retry storm: dispatch_count >> attempt_count
    - OR total job count > 3 (first 3 jobs are free per billing account)
    - Recommendation: Reduce frequency, tune retries, or consolidate jobs

SKU catalog structure (cloudscheduler_sku.json):
{
    "pricing": {
        "free_jobs_per_account": 3,
        "price_per_job_per_month": 0.10
    }
}

Billing model:
  - $0.10 per job per month (first 3 jobs free per billing account)
  - No per-execution charge
  - Paused jobs incur full monthly billing
"""

import logging
from typing import Dict, List, Optional

from ..base_gcp_engine import BaseGCPEngine

logger = logging.getLogger(__name__)

MONTHS_PER_YEAR = 12

# Cloud Scheduler pricing
FREE_JOBS_PER_ACCOUNT = 3
PRICE_PER_JOB_PER_MONTH = 0.10


def _parse_num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


class CloudSchedulerEngine(BaseGCPEngine):
    """GCP Cloud Scheduler rightsizing engine.

    Billing is per-job per-month (first 3 free). The engine calculates
    savings based on job count reduction and frequency optimization.
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

        if rule_code == "CLOUDSCHEDULER_IDLE":
            return self._handle_idle(current_sku, recommendation_template, metrics, resource_data)

        if rule_code == "CLOUDSCHEDULER_OVERPROVISIONED":
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
        Idle: job has zero attempts, is paused, or has 100% error rate.
        Savings = monthly job fee * 12 (if job is beyond the free tier).
        """
        total_jobs_in_account = max(1, _parse_num(resource_data.get("total_jobs_in_account", 1)))
        job_ordinal = _parse_num(resource_data.get("job_ordinal", total_jobs_in_account))

        # Only jobs beyond the free tier incur cost
        is_billable = job_ordinal > FREE_JOBS_PER_ACCOUNT
        monthly_cost = PRICE_PER_JOB_PER_MONTH if is_billable else 0.0
        annual_cost = round(monthly_cost * MONTHS_PER_YEAR, 2)

        job_state = resource_data.get("job_state", "ENABLED")
        is_paused = str(job_state).upper() == "PAUSED"
        error_rate = _parse_num(metrics.get("error_rate_pct", 0))

        if is_paused:
            reason = "paused job (still incurs monthly billing)"
        elif error_rate >= 100.0:
            reason = "job targeting deleted/unreachable resource (100% error rate)"
        else:
            reason = "no successful executions in 14-30 days"

        recommendation = (
            recommendation_template
            or "Idle Cloud Scheduler job — delete {current_sku} ({reason})"
        ).format(current_sku=current_sku, target_sku="N/A", reason=reason)

        details = {
            "recommendation": recommendation,
            "job_name": current_sku,
            "job_state": job_state,
            "is_paused": is_paused,
            "is_billable": is_billable,
            "job_ordinal": job_ordinal,
            "total_jobs_in_account": total_jobs_in_account,
            "error_rate_pct": error_rate,
            "reason": reason,
            "monthly_cost": monthly_cost,
            "annual_cost": annual_cost,
            "monthly_savings": monthly_cost,
            "annual_savings": annual_cost,
            "target_skus": [],
            "is_fallback": False,
        }

        logger.info(f"[CLOUDSCHEDULER_IDLE] {current_sku}: {reason}, annual_savings=${annual_cost}")
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
        Overprovisioned signals:
          - Very high execution frequency (e.g. every 1 min) for low-value work
          - Retry storm: dispatch_count >> attempt_count
          - Total job count > 3 (consolidation opportunity to stay in free tier)

        Primary saving lever: consolidate jobs to stay within the 3-job free tier.
        """
        total_jobs = max(1, _parse_num(resource_data.get("total_jobs_in_account", 1)))
        billable_jobs = max(0, total_jobs - FREE_JOBS_PER_ACCOUNT)
        target_jobs = min(total_jobs, FREE_JOBS_PER_ACCOUNT)
        jobs_to_remove = total_jobs - target_jobs

        current_monthly = round(billable_jobs * PRICE_PER_JOB_PER_MONTH, 4)
        target_monthly = 0.0  # target: stay within free tier
        monthly_savings = current_monthly
        annual_savings = round(monthly_savings * MONTHS_PER_YEAR, 2)
        current_annual = round(current_monthly * MONTHS_PER_YEAR, 2)

        dispatch_count = _parse_num(metrics.get("attempt_dispatch_count", 0))
        attempt_count = _parse_num(metrics.get("attempt_count", 1)) or 1
        retry_ratio = dispatch_count / attempt_count if attempt_count > 0 else 0

        # Per the documentation: dispatch_count >> attempt_count signals a retry storm.
        # A ratio > 3 means each job attempt is being dispatched 3+ times on average,
        # indicating aggressive retry settings are amplifying downstream compute costs.
        if retry_ratio > 3.0:
            action = f"tune retry settings — dispatch/attempt ratio is {retry_ratio:.1f}x (dispatch_count >> attempt_count)"
        elif jobs_to_remove > 0:
            action = f"consolidate {jobs_to_remove} job(s) to stay within free tier ({FREE_JOBS_PER_ACCOUNT} jobs)"
        else:
            action = "reduce execution frequency to lower downstream compute costs"

        recommendation = (
            recommendation_template
            or "Overprovisioned Cloud Scheduler — {action} (current: {current_sku})"
        ).format(current_sku=current_sku, target_sku=f"{target_jobs} jobs", action=action)

        details = {
            "recommendation": recommendation,
            "job_name": current_sku,
            "total_jobs_in_account": total_jobs,
            "billable_jobs": billable_jobs,
            "target_jobs": target_jobs,
            "jobs_to_remove": jobs_to_remove,
            "retry_ratio": round(retry_ratio, 2),
            "action": action,
            "current_monthly_cost": current_monthly,
            "target_monthly_cost": target_monthly,
            "monthly_savings": monthly_savings,
            "annual_savings": annual_savings,
            "target_skus": [{"label": f"{target_jobs} jobs (free tier)", "monthly_cost": 0.0}],
            "is_fallback": False,
        }

        logger.info(
            f"[CLOUDSCHEDULER_OVERPROV] {current_sku}: {total_jobs} jobs -> {target_jobs} jobs, "
            f"annual_savings=${annual_savings}"
        )
        return [recommendation, current_annual, target_monthly * MONTHS_PER_YEAR, annual_savings, details]
