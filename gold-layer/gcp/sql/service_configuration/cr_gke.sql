-- Service Configuration for GCP Google Kubernetes Engine (GKE)
-- Table: service_configuration_v2
-- Pattern: DELETE by service_code, then INSERT

-- Step 1: Delete existing GKE configuration
DELETE FROM service_configuration_v2 WHERE service_code = 'GKE';

-- Step 2: Insert GKE configuration
INSERT INTO service_configuration_v2 (
    service_code,
    service_name,
    cloud_provider,
    category,
    iceberg_metrics_table,
    iceberg_resources_table,
    is_active,
    recommender_class,
    sku_json_file,
    display_service_name
) VALUES (
    'GKE',
    'Google Kubernetes Engine',
    'gcp',
    'Compute',
    'bronze_gcp_metrics_v2',
    'bronze_gcp_gke_clusters',
    true,
    'GKEEngine',
    'cr_gke_sku.json',
    'Google Kubernetes Engine (GKE)'
);
