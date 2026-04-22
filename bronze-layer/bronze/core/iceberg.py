import logging

from bronze.config.job_params import JobParams
from bronze.config.table_config import TableConfig

logger = logging.getLogger(__name__)


def _full_table_name(params: JobParams, table_config: TableConfig) -> str:
    return f"{params.iceberg_catalog}.{params.iceberg_database}.{table_config.table_name}"


def _ensure_table_exists(spark, params: JobParams, table_config: TableConfig) -> None:
    """Create the Iceberg table if it doesn't already exist."""
    full_name = _full_table_name(params, table_config)
    s3_location = f"s3://{params.s3_bucket}/warehouse/{table_config.s3_path_suffix}"

    columns_ddl = ", ".join(
        f"`{col}` {dtype}" for col, dtype in table_config.column_schema.items()
    )
    partition_ddl = ", ".join(f"`{c}`" for c in table_config.partition_columns)

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {columns_ddl}
        )
        USING iceberg
        PARTITIONED BY ({partition_ddl})
        LOCATION '{s3_location}'
    """
    spark.sql(create_sql)


def save_to_iceberg(
    spark,
    records: list,
    table_config: TableConfig,
    params: JobParams,
) -> int:
    """Upsert records into an Iceberg table using MERGE INTO.

    Returns the number of records written.
    """
    import pandas as pd
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

    if not records:
        logger.info("No records to save for %s, skipping.", table_config.table_name)
        return 0

    full_name = _full_table_name(params, table_config)

    pdf = pd.DataFrame(records)
    # Ensure columns match schema order and fill missing with None
    for col in table_config.column_schema:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[[c for c in table_config.column_schema if c in pdf.columns]]

    # Build explicit Spark schema to avoid type inference failures on all-None columns
    type_map = {"string": StringType(), "int": IntegerType(), "long": LongType(), "bigint": LongType(), "double": DoubleType(), "timestamp": TimestampType()}
    spark_schema = StructType([
        StructField(col, type_map.get(dtype, StringType()), nullable=True)
        for col, dtype in table_config.column_schema.items()
    ])

    staging_df = spark.createDataFrame(pdf, schema=spark_schema)
    staging_view = f"staging_{table_config.table_name}"
    staging_df.createOrReplaceTempView(staging_view)

    merge_condition = " AND ".join(
        f"target.`{k}` = source.`{k}`" for k in table_config.key_columns
    )
    update_set = ", ".join(
        f"target.`{c}` = source.`{c}`"
        for c in table_config.column_schema
        if c not in table_config.key_columns
    )
    insert_cols = ", ".join(f"`{c}`" for c in table_config.column_schema)
    insert_vals = ", ".join(f"source.`{c}`" for c in table_config.column_schema)

    merge_sql = f"""
        MERGE INTO {full_name} AS target
        USING {staging_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(merge_sql)

    row_count = len(records)
    logger.info("Saved %d records to %s", row_count, full_name)
    return row_count
