from typing import List

from bronze.services.gcp.cloud_cdn import CLOUD_CDN_SERVICE
from bronze.services.gcp.cloud_dns import CLOUD_DNS_SERVICE
from bronze.services.gcp.cloud_scheduler import CLOUD_SCHEDULER_SERVICE
from bronze.services.gcp.cloud_storage import CLOUD_STORAGE_SERVICE
from bronze.services.gcp.pubsub import PUBSUB_SERVICE
from bronze.services.gcp.types import GCPServiceCatalog


GCP_SERVICES_CATALOG = {
    "CLOUD_SCHEDULER": CLOUD_SCHEDULER_SERVICE,
    "CLOUD_STORAGE": CLOUD_STORAGE_SERVICE,
    "PUBSUB": PUBSUB_SERVICE,
    "CLOUD_CDN": CLOUD_CDN_SERVICE,
    "CLOUD_DNS": CLOUD_DNS_SERVICE,
}


def list_gcp_service_names() -> List[str]:
    return list(GCP_SERVICES_CATALOG.keys())


def get_gcp_service_catalog(service_name: str) -> GCPServiceCatalog:
    return GCP_SERVICES_CATALOG[service_name.strip().upper()]
