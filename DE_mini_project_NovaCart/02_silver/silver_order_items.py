# Databricks notebook source
from pyspark.sql.functions import *

# Step 1: Set catalog & schema
spark.sql("USE CATALOG nova_pipeline")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# Step 2: Read from bronze table
df = spark.table("order_items_bronze")

# Step 3: Select required columns
cleaned_df = df.select(
    col("order_item_id"),
    col("order_id"),
    col("product_id"),
    col("quantity"),
    col("unit_price"),
    col("line_total"),
    col("_modified"),
    col("ingested_at")   
).filter(col("order_item_id").isNotNull())


# COMMAND ----------

# Step 4: Handle missing values
missing_values = ['\n', '?', '', 'null', 'na', 'n/a']

cleaned_df = (
    cleaned_df
    .withColumn(
        "is_valid",
        when(
            col("order_id").isNull() |
            col("product_id").isNull() |
            lower(trim(col("order_id"))).isin(missing_values) |
            lower(trim(col("product_id"))).isin(missing_values) |
            trim(col("order_id")).contains("\\N") |
            trim(col("product_id")).contains("\\N"),
            0
        ).otherwise(1)
    )
    .withColumn(
        "order_id",
        when(
            col("order_id").isNull() |
            lower(trim(col("order_id"))).isin(missing_values) |
            trim(col("order_id")).contains("\\N"),
            None
        ).otherwise(col("order_id"))
    )
    .withColumn(
        "product_id",
        when(
            col("product_id").isNull() |
            lower(trim(col("product_id"))).isin(missing_values) |
            trim(col("product_id")).contains("\\N"),
            None
        ).otherwise(col("product_id"))
    )
)


# COMMAND ----------


# Step 5: Write to silver table
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.silver")

cleaned_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.silver.silver_order_items")

# Step 6: Display result
display(cleaned_df)