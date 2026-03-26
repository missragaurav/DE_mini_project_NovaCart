# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

# 1. Configuration
SOURCE_CATALOG = "nova-cart"
TARGET_CATALOG = "nova_pipeline" # New Catalog Name
SOURCE_SCHEMA = "azure_blob_storage"
BRONZE_SCHEMA = "bronze" 

tables = ["customers", "exchange_rates", "order_items", "orders", "products"]

# 2. Setup Target Structure
# Create the new catalog first (requires 'CREATE CATALOG' permissions)
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{TARGET_CATALOG}`")

# Create the bronze schema within the new catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_CATALOG}`.{BRONZE_SCHEMA}")

# 3. Ingestion Loop
for table_name in tables:
    print(f"Ingesting {table_name} from {SOURCE_CATALOG} to {TARGET_CATALOG}.{BRONZE_SCHEMA}...")
    
    # Read from the original source catalog
    source_path = f"`{SOURCE_CATALOG}`.{SOURCE_SCHEMA}.{table_name}"
    
    # Select data and metadata
    df_bronze = (spark.table(source_path)
        .select("*", "_metadata.file_path")
        .withColumnRenamed("file_path", "source_file")
        .withColumn("ingested_at", current_timestamp())
    )
    
    # Write to the NEW catalog and bronze schema
    target_path = f"`{TARGET_CATALOG}`.{BRONZE_SCHEMA}.{table_name}_bronze"
    
    (df_bronze.write
        .format("delta")
        .mode("overwrite") 
        .option("overwriteSchema", "true")
        .saveAsTable(target_path)
    )

print(f"Ingestion to {TARGET_CATALOG} complete.")
