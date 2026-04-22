from dataclasses import dataclass


@dataclass(frozen=True)
class JobParams:
    """Parsed and validated job parameters for GCP ingestion."""

    job_name: str
    window_days: int
    active_services: list
    project_id: str          # GCP project ID
    client_id: str
    additional_client_id: str  # optional second client_id to duplicate data under
    iceberg_catalog: str
    iceberg_database: str
    s3_bucket: str


def parse_job_params(raw_args: dict) -> JobParams:
    """Parse raw job args dict into a typed JobParams."""
    active_services_raw = raw_args.get("ACTIVE_SERVICES", "")
    active_services = [
        s.strip().upper() for s in active_services_raw.split(",") if s.strip()
    ]

    return JobParams(
        job_name=raw_args.get("JOB_NAME", "bronze-gcp-ingestion"),
        window_days=int(raw_args.get("WINDOW_DAYS", "7")),
        active_services=active_services,
        project_id=raw_args["PROJECT_ID"],
        client_id=raw_args["CLIENT_ID"],
        additional_client_id=raw_args.get("ADDITIONAL_CLIENT_ID", ""),
        iceberg_catalog=raw_args.get("ICEBERG_CATALOG", "glue_catalog"),
        iceberg_database=raw_args.get("ICEBERG_DATABASE", "bronze"),
        s3_bucket=raw_args["S3_BUCKET"],
    )
