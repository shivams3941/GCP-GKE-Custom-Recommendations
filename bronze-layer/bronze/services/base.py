import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from google.cloud import monitoring_v3

from bronze.auth.gcp_auth import get_gcp_credentials
from bronze.config.job_params import JobParams
from bronze.config.table_config import TableConfig
from bronze.core.iceberg import save_to_iceberg
from bronze.core.metadata import stamp_metadata
from bronze.utils.metrics import fetch_metrics

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResourceFetcher:
    """Declares how to fetch one type of resource from a GCP SDK client."""

    sdk_client_class: type
    list_method: str                          # method name on the SDK client
    field_mapping: dict                       # sdk attr dot-path → output column name
    table_config: TableConfig
    filter_fn: Optional[Callable] = None      # optional post-fetch filter
    composite_id_fields: Optional[tuple] = None  # fields joined as resource_id
    parent_id_source: Optional[str] = None    # field supplying parent for child list calls
    paginated: bool = True                    # False for single-response list methods
    fallback_list_method: Optional[str] = None  # fallback for aggregated list (e.g. per-zone)


@dataclass(frozen=True)
class MetricSpec:
    """Declares a single GCP Cloud Monitoring metric to fetch."""

    metric_name: str
    unit: str = ""
    aggregation: str = "Average"   # Average, Maximum, Minimum, Total, Count, Percentile
    interval: str = "PT5M"         # ISO 8601 duration
    percentile: Optional[int] = None  # e.g. 95 for P95, only used with aggregation=Percentile


@dataclass(frozen=True)
class MetricDefinition:
    """Declares what metrics to collect for a service's primary resources."""

    metric_specs: list        # list of MetricSpec
    resource_id_field: str    # field in fetched records holding the GCP resource ID
    table_config: TableConfig


@dataclass(frozen=True)
class ServiceDefinition:
    """Complete declaration of a GCP service — pure config, no logic."""

    name: str                  # e.g. "GKE", "ComputeEngine"
    namespace: str             # GCP metric namespace, e.g. "kubernetes.io"
    resource_fetchers: list    # list of ResourceFetcher
    metrics: Optional[MetricDefinition] = None


def _resolve_attr(obj, dot_path: str):
    """Navigate a dot-separated attribute path on an SDK object or dict.

    Returns None if any segment is missing.
    """
    current = obj
    for segment in dot_path.split("."):
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            current = getattr(current, segment, None)
    return current


class GCPServiceRunner:
    """Executes a ServiceDefinition against GCP: fetches resources, metrics, saves to Iceberg."""

    def __init__(self, spark, params: JobParams, definition: ServiceDefinition, dry_run: bool = False):
        self.spark = spark
        self.params = params
        self.definition = definition
        self.dry_run = dry_run
        self.credentials = get_gcp_credentials()

    def run(self) -> dict:
        """Run the service. Returns fetched data keyed by table name.

        In dry_run mode, skips Iceberg saves and just returns the data.
        """
        logger.info("Running GCP service: %s", self.definition.name)

        results = {}
        primary_resources = []

        for i, fetcher in enumerate(self.definition.resource_fetchers):
            resources = self._fetch_resources(fetcher, parent_resources=primary_resources if fetcher.parent_id_source else None)
            results[fetcher.table_config.table_name] = resources
            if not self.dry_run:
                save_to_iceberg(self.spark, resources, fetcher.table_config, self.params)
            if i == 0:
                primary_resources = resources

        if self.definition.metrics and primary_resources:
            metrics = self._fetch_all_metrics(primary_resources)
            results[self.definition.metrics.table_config.table_name] = metrics
            if not self.dry_run:
                save_to_iceberg(self.spark, metrics, self.definition.metrics.table_config, self.params)

        logger.info("Completed GCP service: %s", self.definition.name)
        return results

    def _build_client(self, sdk_client_class):
        """Instantiate a GCP SDK client with service account credentials."""
        return sdk_client_class(credentials=self.credentials)

    def _fetch_resources(self, fetcher: ResourceFetcher, parent_resources: list = None) -> list:
        """Fetch resources using the SDK client and method declared in the fetcher."""
        job_runtime_utc = datetime.now(timezone.utc)
        records = []

        # If this fetcher depends on a parent (e.g. node pools per cluster),
        # iterate over each parent resource and call the list method per parent.
        if fetcher.parent_id_source and parent_resources:
            for parent in parent_resources:
                parent_id = parent.get(fetcher.parent_id_source)
                if not parent_id:
                    continue
                raw_items = self._call_list_method(fetcher, parent_id=parent_id)
                records.extend(self._map_items(raw_items, fetcher, job_runtime_utc, parent))
        else:
            raw_items = self._call_list_method(fetcher)
            records.extend(self._map_items(raw_items, fetcher, job_runtime_utc))

        logger.info("Fetched %d records via %s", len(records), fetcher.list_method)
        return records

    def _call_list_method(self, fetcher: ResourceFetcher, parent_id: str = None) -> list:
        """Call the SDK list method, handling pagination and parent-scoped calls."""
        client = self._build_client(fetcher.sdk_client_class)
        list_fn = getattr(client, fetcher.list_method)

        try:
            if parent_id:
                raw = list_fn(parent=parent_id)
            else:
                raw = list_fn(parent=f"projects/{self.params.project_id}")

            # aggregated_list returns (zone, items) pairs — flatten them
            items = []
            for entry in raw:
                if hasattr(entry, "instances") or hasattr(entry, "items"):
                    # aggregated list response
                    scoped = getattr(entry, "instances", None) or getattr(entry, "items", None)
                    if scoped:
                        items.extend(scoped)
                else:
                    items.append(entry)

            return items if fetcher.paginated else list(raw)

        except Exception:
            logger.warning(
                "Failed to call %s on %s",
                fetcher.list_method, fetcher.sdk_client_class.__name__,
                exc_info=True,
            )
            return []

    def _map_items(self, raw_items, fetcher: ResourceFetcher, job_runtime_utc: datetime, parent: dict = None) -> list:
        """Map raw SDK objects to output record dicts."""
        records = []
        for item in raw_items:
            record = {}

            for sdk_path, output_field in fetcher.field_mapping.items():
                value = _resolve_attr(item, sdk_path)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                elif value is not None:
                    value = str(value) if not isinstance(value, (int, float, bool)) else value
                record[output_field] = value

            # Compose resource_id from multiple fields if declared
            if fetcher.composite_id_fields:
                parts = []
                for f in fetcher.composite_id_fields:
                    # check record first, then parent
                    val = record.get(f) or (parent.get(f) if parent else None) or ""
                    parts.append(str(val))
                record["resource_id"] = ".".join(parts)

            # Derive resource_name from the primary name field if not already set
            if not record.get("resource_name"):
                record["resource_name"] = (
                    record.get("cluster_name")
                    or record.get("node_pool_name")
                    or record.get("resource_id", "")
                )

            # Inherit project_id and location from parent if not on the item itself
            if parent:
                for inherit_field in ("project_id", "location", "cluster_name"):
                    if not record.get(inherit_field) and parent.get(inherit_field):
                        record[inherit_field] = parent[inherit_field]

            stamp_metadata(record, self.params)
            record["service_name"] = self.definition.name
            record["job_runtime_utc"] = job_runtime_utc

            if fetcher.filter_fn and not fetcher.filter_fn(record):
                continue

            records.append(record)
        return records

    def _fetch_all_metrics(self, primary_resources: list) -> list:
        """Fetch GCP Cloud Monitoring metrics for all primary resources in parallel."""
        metrics_def = self.definition.metrics
        job_runtime_utc = datetime.now(timezone.utc)

        monitoring_client = monitoring_v3.MetricServiceClient(credentials=self.credentials)

        unit_by_metric = {spec.metric_name: spec.unit for spec in metrics_def.metric_specs}

        def _fetch_one(resource: dict) -> list:
            resource_id = resource.get(metrics_def.resource_id_field)
            if not resource_id:
                return []

            rows = fetch_metrics(
                monitoring_client=monitoring_client,
                project_id=self.params.project_id,
                resource_id=resource_id,
                metric_specs=metrics_def.metric_specs,
                window_days=self.params.window_days,
            )
            for row in rows:
                stamp_metadata(row, self.params)
                row["resource_id"] = resource_id
                row["resource_name"] = resource.get("resource_name", "")
                row["service_name"] = self.definition.name
                row["namespace"] = self.definition.namespace
                row["region"] = resource.get("location", resource.get("region", ""))
                row["unit"] = unit_by_metric.get(row["metric_name"], "")
                row["metric_unit"] = row["unit"]
                row["date"] = row["timestamp"]
                row["metric_date"] = row["timestamp"][:10]
                row["job_runtime_utc"] = job_runtime_utc
                row.pop("timestamp", None)
            return rows

        all_metrics = []
        max_workers = min(20, len(primary_resources))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one, r): r for r in primary_resources}
            for future in as_completed(futures):
                try:
                    all_metrics.extend(future.result())
                except Exception:
                    logger.warning(
                        "Failed to fetch metrics for %s",
                        futures[future].get("resource_id", "unknown"),
                        exc_info=True,
                    )

        logger.info(
            "Fetched %d metric rows for %d resources",
            len(all_metrics), len(primary_resources),
        )
        return all_metrics
