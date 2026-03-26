# Databricks notebook source
from pyspark.sql.functions import *

# Step 1: Set catalog & schema
spark.sql("USE CATALOG nova_pipeline")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# Step 2: Read from bronze table
df = spark.table("exchange_rates_bronze")

# Step 3: Select required columns
cleaned_df = df.select(
    col("currency_code"),
    col("exchange_rate_to_usd"),
    col("effective_date"),
    col("_modified"),
    col("ingested_at")   
)

# COMMAND ----------

# Step 4: Handle multiple date formats
parsed_date = coalesce(
    expr("try_to_date(effective_date, 'MM/dd/yyyy')"),
    expr("try_to_date(effective_date, 'dd/MM/yyyy')"),
    expr("try_to_date(effective_date, 'yyyy-MM-dd')"),
    expr("try_to_date(effective_date, 'MM-dd-yyyy')"),
    expr("try_to_date(effective_date, 'dd-MM-yyyy')")
)


# COMMAND ----------

# Step 5: Apply transformation (yyyy-MM-dd)
transformed_df = (
    cleaned_df
    .withColumn("parsed_date", parsed_date)
    .withColumn(
        "effective_date",
        when(
            col("parsed_date").isNotNull(),
            date_format(col("parsed_date"), "yyyy-MM-dd")   # ✅ fixed format
        ).otherwise(col("effective_date"))
    )
    .drop("parsed_date")
)

# COMMAND ----------


# Step 6: Create silver schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.silver")

# Step 7: Write to silver table
transformed_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("nova_pipeline.silver.silver_exchange_rates")   # ✅ fixed path

# Step 8: Display result
display(transformed_df)