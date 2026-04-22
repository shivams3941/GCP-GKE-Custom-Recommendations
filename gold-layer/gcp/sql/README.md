# SQL Configuration Files — GCP GKE

This folder contains SQL files for populating RDS tables used by the GKE rightsizing engine in production.

## Structure

```
sql/
├── service_configuration/    -> INSERT statements for service_configuration_v2 table
│   └── cr_gke.sql
│
└── threshold_rules/          -> INSERT statements for base_finops_threshold_rules_v2 table
    └── cr_gke.sql
```

## Tables

### service_configuration_v2
Defines GCP services and their metadata — which Iceberg tables to read from, which engine
class to use, and which SKU JSON file to load.

### base_finops_threshold_rules_v2
Defines threshold rules for each service — metric conditions, operators, thresholds,
recommendation templates, and savings formulas.

## Naming Convention

Files follow the same pattern as the rest of the project:
- `cr_{service}.sql` (e.g., `cr_gke.sql`)

## Usage

Run individual SQL files against your RDS database:

```bash
psql -h your-rds-host -U username -d database -f sql/service_configuration/cr_gke.sql
psql -h your-rds-host -U username -d database -f sql/threshold_rules/cr_gke.sql
```

Or run both together for GKE:

```bash
psql -h your-rds-host -U username -d database \
  -f sql/service_configuration/cr_gke.sql \
  -f sql/threshold_rules/cr_gke.sql
```

## Adding a New GCP Service

1. Create `sql/service_configuration/cr_{service}.sql` with INSERT for service_configuration_v2
2. Create `sql/threshold_rules/cr_{service}.sql` with INSERT(s) for base_finops_threshold_rules_v2
3. Scripts use DELETE + INSERT pattern to stay idempotent — safe to re-run
