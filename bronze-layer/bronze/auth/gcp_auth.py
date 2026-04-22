import os
import logging

from google.oauth2 import service_account

logger = logging.getLogger(__name__)

_GCP_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

# Path to the service account key file.
# TODO: replace with AWS Secrets Manager lookup via bronze.auth.secrets.get_secret_json()
_SERVICE_ACCOUNT_KEY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../../bronze/services/gcp/devops-internal-439011-f4e928045fd3.json",
)


def get_gcp_credentials() -> service_account.Credentials:
    """Load GCP credentials from the local service account key file.

    TODO: In production, replace this with:
        from bronze.auth.secrets import get_secret_json
        secret = get_secret_json("gcp/devops-internal/service-account")
        return service_account.Credentials.from_service_account_info(secret, scopes=_GCP_SCOPES)
    """
    key_path = os.path.normpath(_SERVICE_ACCOUNT_KEY_PATH)
    logger.info("Loading GCP credentials from: %s", key_path)
    return service_account.Credentials.from_service_account_file(
        key_path,
        scopes=_GCP_SCOPES,
    )
