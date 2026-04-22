"""GCP service definitions."""
from bronze.services.gcp.vm import COMPUTE_ENGINE_SERVICE  # noqa: F401
from bronze.services.gcp.cloudrun import CLOUDRUN_SERVICE  # noqa: F401
from bronze.services.gcp.cloudnat import CLOUDNAT_SERVICE  # noqa: F401
from bronze.services.gcp.cloudbuild import CLOUDBUILD_SERVICE  # noqa: F401
from bronze.services.gcp.gke import GKE_SERVICE  # noqa: F401
from bronze.services.registry import register

register(COMPUTE_ENGINE_SERVICE)
register(CLOUDRUN_SERVICE)
register(CLOUDNAT_SERVICE)
register(CLOUDBUILD_SERVICE)
register(GKE_SERVICE)
