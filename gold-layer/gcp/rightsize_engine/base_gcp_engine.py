"""
Base GCP Rightsizing Engine
============================

Mirrors the pattern of BaseAzureEngine but adapted for GCP services.
Each GCP service engine (GKE, VM, CloudSQL, etc.) extends this class.

Return Format:
    find_rightsize_candidate() returns either:
    - List: [recommendation_text, current_annual_price, target_annual_price, annual_savings, details_dict]
    - None: if no suitable candidate is found
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseGCPEngine:
    """Base class for GCP rightsizing engines.

    Subclasses must override:
        - find_rightsize_candidate(): Main orchestration method

    Subclasses may optionally override:
        - _find_candidate(): Find a smaller SKU that fits the workload
        - _calculate_yearly_price(): Calculate yearly price of target SKU

    Return Format:
        find_rightsize_candidate() returns:
        - List[str, float, float, float, dict]:
            [recommendation_text, current_annual_price, target_annual_price, annual_savings, details]
        - None: if no candidate found
    """

    def __init__(self, sku_path: str, json_filename: str):
        """
        Args:
            sku_path: Local directory path or S3 prefix (s3://bucket/prefix/).
            json_filename: SKU JSON filename (e.g. "cr_gke_sku.json").
        """
        self.sku_path = sku_path
        self.json_filename = json_filename
        logger.info(f"Initializing {self.__class__.__name__} with sku_path={sku_path}, json_filename={json_filename}")
        self.sku_catalog = self._load_sku_catalog()

    # ------------------------------------------------------------------
    # Catalog loading
    # ------------------------------------------------------------------

    def _load_sku_catalog(self) -> dict:
        if self.sku_path.startswith("s3://"):
            return self._load_from_s3()
        return self._load_from_local()

    def _load_from_s3(self) -> dict:
        try:
            import boto3
            from botocore.config import Config
            s3 = boto3.client("s3", config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}))
            path = self.sku_path.replace("s3://", "")
            bucket = path.split("/")[0]
            prefix = "/".join(path.split("/")[1:])
            key = prefix + self.json_filename
            logger.info(f"Loading SKU catalog from s3://{bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            catalog = json.loads(response["Body"].read().decode("utf-8"))
            logger.info(f"Loaded {len(catalog)} entries from s3://{bucket}/{key}")
            return catalog
        except Exception as e:
            logger.error(f"Failed to load SKU catalog from S3: {e}")
            raise

    def _load_from_local(self) -> dict:
        file_path = os.path.join(self.sku_path, self.json_filename)
        logger.debug(f"Loading SKU catalog from local path={file_path}")
        try:
            with open(file_path, "r") as f:
                catalog = json.load(f)
            logger.info(f"Loaded SKU catalog with {len(catalog)} entries from {file_path}")
            return catalog
        except Exception as e:
            logger.error(f"Failed to load SKU catalog from {file_path}: {e}")
            raise

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def find_rightsize_candidate(
        self,
        current_sku: str,
        region: str,
        rule_code: str,
        recommendation_template: str,
        metrics: Dict[str, float] = None,
        resource_data: Dict[str, Any] = None,
        **kwargs,
    ) -> Optional[List]:
        """Find a rightsize candidate for the given resource.

        Args:
            current_sku: Current machine type / SKU (e.g. "n2-standard-8")
            region: GCP region (e.g. "us-central1")
            rule_code: Rule code (e.g. "GKE_IDLE", "GKE_OVERPROVISIONED")
            recommendation_template: Template string with {current_sku}/{target_sku} placeholders
            metrics: Dict of aggregated metric values
            resource_data: Dict of resource attributes from the bronze table
            **kwargs: Additional service-specific arguments

        Returns:
            [recommendation_text, current_annual_price, target_annual_price, annual_savings, details]
            or None
        """
        candidate = self._find_candidate(
            current_sku, region, rule_code, recommendation_template,
            metrics=metrics, resource_data=resource_data, **kwargs
        )
        if candidate:
            return candidate
        return None

    # ------------------------------------------------------------------
    # Overridable hooks
    # ------------------------------------------------------------------

    def _find_candidate(
        self,
        current_sku: str,
        region: str,
        rule_code: str,
        recommendation_template: str,
        metrics: Dict[str, float] = None,
        resource_data: Dict[str, Any] = None,
        **kwargs,
    ) -> Optional[List]:
        """Override in subclass to implement service-specific candidate logic."""
        return None

    def _calculate_yearly_price(self, target_candidate: Dict, **kwargs) -> float:
        """Default: hourly_price * 8760. Override if needed."""
        if not target_candidate:
            return 0.0
        return round(target_candidate.get("hourly_price", 0.0) * 8760, 2)
