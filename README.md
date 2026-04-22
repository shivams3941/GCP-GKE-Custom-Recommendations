# GCP GKE Custom Recommendations

A comprehensive data pipeline for Google Cloud Platform (GCP) Google Kubernetes Engine (GKE) resource analysis and custom recommendations. This project implements a bronze-gold architecture for collecting GKE metrics and generating optimization recommendations.

## Architecture

This project follows a bronze-gold data architecture pattern:

- **Bronze Layer**: Raw data ingestion and transformation from GCP APIs
- **Gold Layer**: Business logic and recommendation engine

### Bronze Layer

The bronze layer handles:
- Authentication with GCP services
- Data collection from GKE clusters and node pools
- Metrics collection from Cloud Monitoring
- Data transformation and storage in Iceberg tables

#### Key Components:
- `bronze/auth/gcp_auth.py` - GCP authentication management
- `bronze/services/gcp/gke.py` - GKE cluster and node pool data collection
- `bronze/core/spark.py` - Spark session management
- `bronze/core/iceberg.py` - Iceberg table operations

### Gold Layer

The gold layer contains:
- Recommendation engine for GKE resource optimization
- Custom right-sizing algorithms
- Business logic for cost optimization

#### Key Components:
- `gcp/rightsize_engine/services/cr_gke_engine.py` - GKE recommendation engine
- `gcp/resources/scripts/cr_gke_fetch.py` - GKE resource fetching scripts

## Features

- **Multi-project Support**: Can analyze GKE resources across multiple GCP projects
- **Comprehensive Metrics**: Collects CPU, memory, and storage metrics
- **Node Pool Analysis**: Detailed analysis of GKE node pool configurations
- **Cost Optimization**: Generates recommendations for optimal resource sizing
- **Autopilot Detection**: Identifies and analyzes GKE Autopilot clusters
- **Metrics Integration**: Leverages Cloud Monitoring metrics for utilization analysis

## Data Collected

### GKE Clusters
- Cluster configuration and metadata
- Network settings
- Version information
- Status and health
- Autopilot configuration

### Node Pools
- Machine types and sizing
- Autoscaling configuration
- Disk configurations
- Spot/preemptible settings

### Metrics
- CPU utilization (allocatable and request)
- Memory utilization (allocatable and request)
- Storage utilization
- Container-level metrics

## Prerequisites

- Python 3.8+
- GCP project with appropriate permissions
- Service account with GKE and Cloud Monitoring access
- Apache Spark environment
- Iceberg table storage

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd GCP-GKE-Custom-Recommendations
```

2. Install dependencies:
```bash
pip install -r gold-layer/requirements.txt
```

3. Configure GCP authentication:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

## Configuration

### GCP Authentication
The project uses service account authentication. Store your service account key in AWS Secrets Manager under the secret name defined in `GCP_SECRET_NAME`.

### Table Configurations
Table schemas and storage paths are defined in the respective service definition files. Key tables include:
- `bronze_gcp_gke_clusters` - GKE cluster data
- `bronze_gcp_gke_node_pools` - Node pool configurations
- `bronze_gcp_metrics_v2` - Cloud Monitoring metrics

## Usage

### Running Data Collection
```python
from bronze.services.gcp.gke import GKE_SERVICE
from bronze.core.spark import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Run GKE data collection
# Implementation depends on your specific orchestration framework
```

### Generating Recommendations
```python
from gcp.rightsize_engine.services.cr_gke_engine import CRGKEEngine

# Initialize recommendation engine
engine = CRGKEEngine()

# Generate recommendations
recommendations = engine.generate_recommendations(cluster_data, metrics_data)
```

## Development

### Project Structure
```
GCP-GKE-Custom-Recommendations/
|-- bronze-layer/
|   |-- bronze/
|   |   |-- auth/          # GCP authentication
|   |   |-- config/        # Configuration management
|   |   |-- core/          # Core utilities (Spark, Iceberg)
|   |   |-- services/      # Service definitions
|   |   |-- utils/         # Utility functions
|   |-- tests/             # Bronze layer tests
|-- gold-layer/
|   |-- gcp/
|   |   |-- rightsize_engine/  # Recommendation engine
|   |   |-- resources/        # Resource scripts
|   |-- requirements.txt
```

### Testing
Run tests using:
```bash
python -m pytest bronze-layer/tests/
```

### Local Development
For local development, use the provided local development script:
```bash
python bronze-layer/tests/local_run_gcp_gke.py
```

## Monitoring and Logging

The project integrates with:
- Cloud Logging for application logs
- Cloud Monitoring for metrics collection
- Custom metrics for pipeline monitoring

## Security

- Service account authentication
- Secrets management through AWS Secrets Manager
- Principle of least privilege for GCP permissions
- Data encryption in transit and at rest

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is proprietary and confidential.

## Support

For questions or support, please contact the infrastructure team.

## Roadmap

- [ ] Support for additional GCP services
- [ ] Enhanced recommendation algorithms
- [ ] Real-time monitoring dashboard
- [ ] Multi-cloud support
- [ ] Cost forecasting capabilities
