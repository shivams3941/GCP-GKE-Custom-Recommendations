import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from google.cloud import monitoring_v3
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)


def _parse_interval_minutes(interval: str) -> int:
    """Parse an ISO 8601 duration (e.g. PT5M, PT1H, P1D) into minutes."""
    m = re.match(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?", interval)
    if not m:
        return 5
    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    return days * 1440 + hours * 60 + minutes


def _floor_timestamp(ts: datetime, interval_minutes: int) -> datetime:
    """Round a timestamp down to the nearest interval boundary."""
    total_minutes = ts.hour * 60 + ts.minute
    floored_minutes = (total_minutes // interval_minutes) * interval_minutes
    return ts.replace(
        hour=floored_minutes // 60,
        minute=floored_minutes % 60,
        second=0,
        microsecond=0,
    )


def _aggregation_reducer(aggregation: str, percentile: int = None):
    """Map aggregation string to GCP Cloud Monitoring Aggregation reducer."""
    mapping = {
        "Average": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
        "Maximum": monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
        "Minimum": monitoring_v3.Aggregation.Reducer.REDUCE_MIN,
        "Total": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
        "Count": monitoring_v3.Aggregation.Reducer.REDUCE_COUNT,
        "Percentile": monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99,  # overridden below
    }
    if aggregation == "Percentile" and percentile:
        # GCP supports p50, p99 reducers
        percentile_map = {
            50: monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_50,
            99: monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99,
        }
        return percentile_map.get(percentile, monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99)
    return mapping.get(aggregation, monitoring_v3.Aggregation.Reducer.REDUCE_MEAN)


def fetch_metrics(
    monitoring_client: monitoring_v3.MetricServiceClient,
    project_id: str,
    resource_id: str,
    metric_specs: list,
    window_days: int,
) -> list:
    """Fetch metrics from GCP Cloud Monitoring for a given resource.

    Each MetricSpec declares its own metric_name, aggregation, interval, and optional percentile.
    Returns a list of row dicts with metric_name, aggregation_type, timestamp, metric_value.
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=window_days)

    end_ts = Timestamp()
    end_ts.FromDatetime(end_time)
    start_ts = Timestamp()
    start_ts.FromDatetime(start_time)

    interval = monitoring_v3.TimeInterval(
        end_time=end_ts,
        start_time=start_ts,
    )

    project_name = f"projects/{project_id}"
    all_rows = []

    # Group by (metric_name, aggregation, interval_str, percentile) to deduplicate calls
    seen = set()
    for spec in metric_specs:
        key = (spec.metric_name, spec.aggregation, spec.interval, getattr(spec, "percentile", None))
        if key in seen:
            continue
        seen.add(key)

        interval_minutes = _parse_interval_minutes(spec.interval)
        alignment_period = interval_minutes * 60  # seconds

        aggregation = monitoring_v3.Aggregation(
            alignment_period={"seconds": alignment_period},
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            cross_series_reducer=_aggregation_reducer(spec.aggregation, getattr(spec, "percentile", None)),
            group_by_fields=["resource.labels.cluster_name"],
        )

        try:
            results = monitoring_client.list_time_series(
                request={
                    "name": project_name,
                    "filter": f'metric.type = "{spec.metric_name}"',
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    "aggregation": aggregation,
                }
            )

            for ts in results:
                for point in ts.points:
                    point_dt = point.interval.end_time.ToDatetime(tzinfo=timezone.utc)
                    floored = _floor_timestamp(point_dt, interval_minutes)
                    value = (
                        point.value.double_value
                        or point.value.int64_value
                        or point.value.distribution_value.mean
                        or 0.0
                    )
                    all_rows.append({
                        "metric_name": spec.metric_name,
                        "aggregation_type": spec.aggregation,
                        "timestamp": floored.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "metric_value": round(float(value), 6),
                    })

        except Exception:
            logger.warning(
                "Failed to fetch metric '%s' (%s) for resource %s",
                spec.metric_name, spec.aggregation, resource_id,
                exc_info=True,
            )

    return all_rows
