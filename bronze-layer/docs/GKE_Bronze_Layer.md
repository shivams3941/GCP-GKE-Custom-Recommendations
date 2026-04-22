# GKE Bronze Layer — Technical Overview

## What is this?

This project ingests Google Kubernetes Engine (GKE) resource and metrics data from GCP into an Iceberg data lake (on S3 via AWS Glue). The data is used to generate cost optimisation recommendations — identifying idle and overprovisioned GKE clusters, node pools, and workloads.

---

## Project Structure

```
bronze/
  auth/
    gcp_auth.py          # GCP authentication (service account key)
  config/
    job_params.py        # Job parameters (project_id, client_id, etc.)
    table_config.py      # Iceberg table schema definitions
  core/
    metadata.py          # Stamps standard fields (client_id, cloud_name, etc.)
    iceberg.py           # Writes data to Iceberg via Spark MERGE INTO
    spark.py             # Creates Spark session for Glue
  services/
    base.py              # GCPServiceRunner — the engine that runs everything
    registry.py          # Service registry (lookup by name)
    gcp/
      gke.py             # GKE service definition (tables, field mappings, metrics)
      vm.py              # Compute Engine service definition
      cloudrun.py        # Cloud Run service definition
      cloudnat.py        # Cloud NAT service definition
      cloudbuild.py      # Cloud Build service definition
  utils/
    metrics.py           # Fetches metrics from GCP Cloud Monitoring
tests/
  local_run_gcp_gke.py   # Local validation script — no Spark/Iceberg needed
```

---

## How Authentication Works

GCP credentials are loaded from a local service account key file:

```
bronze/services/gcp/devops-internal-439011-f4e928045fd3.json
```

The `get_gcp_credentials()` function in `bronze/auth/gcp_auth.py` reads this file and returns a `google.oauth2.service_account.Credentials` object scoped to `https://www.googleapis.com/auth/cloud-platform`.

> **Future plan:** Replace the local file with AWS Secrets Manager. The key will be stored under `gcp/devops-internal/service-account` and fetched at runtime — no file on disk.

---

## How the Service Definition Works

Every GCP service (GKE, VM, CloudRun, etc.) is declared as pure config in its own file. No logic lives in the service files — only table schemas, field mappings, and metric specs.

`bronze/services/gcp/gke.py` declares three things:

### 1. Table Configs
Defines the Iceberg table schema for each resource type:
- `GKE_CLUSTERS_TABLE` → `bronze_gcp_gke_clusters`
- `GKE_NODE_POOLS_TABLE` → `bronze_gcp_gke_node_pools`
- `GKE_METRICS_TABLE` → `bronze_gcp_metrics_v2`

Each table config specifies column names, types, key columns (for upsert), and partition columns.

### 2. Field Mappings
Maps GCP SDK attribute paths to output column names:

```python
GKE_CLUSTER_FIELD_MAPPING = {
    "name": "cluster_name",
    "location": "location",
    "autopilot.enabled": "autopilot_enabled",
    "release_channel.channel": "release_channel",
    ...
}
```

Dot-paths like `"autopilot.enabled"` navigate nested SDK objects automatically.

`resource_id` is composed from multiple fields using `composite_id_fields`:
- Cluster: `project_id.location.cluster_name`
- Node pool: `project_id.location.cluster_name.node_pool_name`

This ensures uniqueness across multi-project and multi-region setups.

### 3. Metric Specs
Declares which Cloud Monitoring metrics to fetch, with aggregation type and interval:

```python
MetricSpec("kubernetes.io/node/cpu/allocatable_utilization", unit="Ratio", aggregation="Average", interval="PT5M")
MetricSpec("kubernetes.io/node/cpu/allocatable_utilization", unit="Ratio", aggregation="Percentile", percentile=95, interval="PT5M")
```

---

## Metrics — What We Collect and Why

All metrics come from GCP Cloud Monitoring under the `kubernetes.io` namespace.

### Idle Detection Signals

| Metric | Signal | Threshold |
|--------|--------|-----------|
| `kubernetes.io/node/cpu/allocatable_utilization` | Node CPU nearly unused | P95 < 10% over 14 days |
| `kubernetes.io/node/memory/allocatable_utilization` | Node memory nearly unused | P95 < 10% over 14 days |
| `kubernetes.io/container/cpu/request_utilization` | Container CPU negligible | P95 < 5% over 7–14 days |
| `kubernetes.io/container/memory/request_utilization` | Container memory negligible | P95 < 5% over 7–14 days |
| `kubernetes.io/pod/volume/total_bytes` | PVC not used by any pod | = 0 over 7–14 days |

### Overprovisioned Detection Signals

| Metric | Signal | Threshold |
|--------|--------|-----------|
| `kubernetes.io/container/cpu/request_utilization` | CPU requests far exceed actual usage | P95 < 25% over 14–30 days |
| `kubernetes.io/container/memory/request_utilization` | Memory requests far exceed actual usage | P95 < 25% over 14–30 days |
| `kubernetes.io/node/cpu/allocatable_utilization` | Node pool oversized vs workload | P95 < 30% over 14 days |
| `kubernetes.io/node/memory/allocatable_utilization` | Node memory capacity excess | P95 < 30% over 14 days |
| `kubernetes.io/container/cpu/limit_utilization` | CPU limits set far above actual usage | P95 < 15% over 14 days |

---

## How the Runner Works (GCPServiceRunner)

`bronze/services/base.py` contains `GCPServiceRunner` — the single class that executes a `ServiceDefinition`.

```
GCPServiceRunner.run()
  ├── For each ResourceFetcher:
  │     ├── Build SDK client with credentials
  │     ├── Call list method (e.g. list_clusters, list_node_pools)
  │     ├── If parent_id_source set → iterate parent resources first (node pools per cluster)
  │     ├── Map SDK fields via field_mapping
  │     ├── Compose resource_id from composite_id_fields
  │     ├── Stamp metadata (client_id, account_id, cloud_name, year_month, etc.)
  │     └── Save to Iceberg (MERGE INTO upsert)
  └── For MetricDefinition:
        ├── Fetch Cloud Monitoring metrics per resource (parallel threads)
        ├── Stamp metadata on each metric row
        └── Save to Iceberg
```

In `dry_run=True` mode, all fetching happens but nothing is written to Iceberg — useful for local testing.

---

## How to Run Locally (Validation)

No Spark or Iceberg needed. From the project root:

```cmd
python tests/local_run_gcp_gke.py
```

This will:
1. Load credentials from the service account key file
2. Fetch all GKE clusters in the project
3. Fetch node pools for each cluster
4. Fetch the metrics specs from Cloud Monitoring
5. Print everything to the console

### Example Output
```
Authenticated as: billing-jay@devops-internal-439011.iam.gserviceaccount.com

Cluster: devops-internal-439011.us-central1-a.custom-recommendation-testing
  status          : RUNNING
  location        : us-central1-a
  master_version  : 1.35.1-gke.1396002
  node_count      : 1
  autopilot       : False
  release_channel : REGULAR
  Node pools (1):
    - devops-internal-439011.us-central1-a.custom-recommendation-testing.default-pool
        status       : RUNNING
        machine_type : e2-medium
        autoscaling  : False
        min/max nodes: 0 / 0
  Metrics (last 7 days, sample of 3 specs):
    kubernetes.io/node/cpu/allocatable_utilization (Average):
      2026-04-06 11:36  →  0.105809 Ratio
    kubernetes.io/node/memory/allocatable_utilization (Average):
      2026-04-06 11:36  →  0.084139 Ratio
    kubernetes.io/container/cpu/request_utilization (Average):
      2026-04-06 11:36  →  0.262513 Ratio
```

---

## How to Run on Glue (Production)

Once local validation passes, the full pipeline runs on AWS Glue with Spark:

```python
from bronze.core.spark import create_spark_session
from bronze.config.job_params import parse_job_params
from bronze.services.base import GCPServiceRunner
from bronze.services.gcp.gke import GKE_SERVICE

params = parse_job_params(glue_args)
spark = create_spark_session(params)

runner = GCPServiceRunner(spark=spark, params=params, definition=GKE_SERVICE)
runner.run()
```

Glue job args required:
| Arg | Description |
|-----|-------------|
| `PROJECT_ID` | GCP project ID |
| `CLIENT_ID` | Client identifier for multi-tenant support |
| `WINDOW_DAYS` | How many days of metrics to fetch (default: 7) |
| `S3_BUCKET` | S3 bucket for Iceberg warehouse |
| `ICEBERG_CATALOG` | Glue catalog name |
| `ICEBERG_DATABASE` | Glue database name |

---

## Current Status

| Area | Status |
|------|--------|
| GKE cluster fetching | Working |
| Node pool fetching | Working |
| Cloud Monitoring metrics | Working |
| Local validation script | Working |
| Iceberg write (Glue) | Ready — pending Glue job setup |
| AWS Secrets Manager for credentials | Planned |

---

## Future Plans

- Replace local service account key file with AWS Secrets Manager
- Add 1-minute granularity metric specs for finer signal resolution
- Integrate P95 percentile metrics into recommendation engine
- Extend to additional GCP services (VM, CloudRun, CloudNAT, CloudBuild)
