# Databricks notebook source
from pyspark.sql.functions import *

# Step 1: Set catalog & schema
spark.sql("USE CATALOG nova_pipeline")
spark.sql("USE SCHEMA bronze")

# Step 2: Read from bronze table
df = spark.table("products_bronze")


# COMMAND ----------

# Step 3: Select required columns
cleaned_df = df.select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("price"),
    col("currency"),
    col("country_code"),
    col("_modified"),
    col("ingested_at")   
).filter(col("product_id").isNotNull())


# COMMAND ----------

# Step 4: Handle missing values
missing_values = ['\n', '?', '', 'null', 'na', 'n/a']

cleaned_df = (
    cleaned_df
    .withColumn(
        "is_valid",
        when(
            col("product_name").isNull() |
            col("category").isNull() |
            col("country_code").isNull() |
            lower(trim(col("product_name"))).isin(missing_values) |
            lower(trim(col("country_code"))).isin(missing_values) |
            lower(trim(col("category"))).isin(missing_values) |
            trim(col("product_name")).contains("\\N") |
            trim(col("category")).contains("\\N") |
            trim(col("country_code")).contains("\\N"),
            0
        ).otherwise(1)
    )
    .withColumn(
        "product_name",
        when(
            col("product_name").isNull() |
            lower(trim(col("product_name"))).isin(missing_values) |
            trim(col("product_name")).contains("\\N"),
            None
        ).otherwise(col("product_name"))
    )
    .withColumn(
        "category",
        when(
            col("category").isNull() |
            lower(trim(col("category"))).isin(missing_values) |
            trim(col("category")).contains("\\N"),
            None
        ).otherwise(col("category"))
    )
    .withColumn(
        "country_code",
        when(
            col("country_code").isNull() |
            lower(trim(col("country_code"))).isin(missing_values) |
            trim(col("country_code")).contains("\\N"),
            None
        ).otherwise(col("country_code"))
    )
)


# COMMAND ----------


# Step 5: Create silver schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.silver")

# Step 6: Write to silver table
cleaned_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.silver.silver_products")

# Step 7: Display result
display(cleaned_df)