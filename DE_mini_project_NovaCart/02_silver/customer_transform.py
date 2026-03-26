# Databricks notebook source
from pyspark.sql.functions import *


# Step 1: Set correct catalog & schema
spark.sql("USE CATALOG nova_pipeline")
spark.sql("USE SCHEMA bronze")


# Step 2: Read from bronze table
df = spark.table("customers_bronze")


# COMMAND ----------

# Step 3: Select & clean required columns

cleaned_df = df.select(
    col("customer_id"),
    col("customer_name"),
    col("email"),
    col("registration_date"),
    col("country_code"),
    col("channel"),
    col("_modified"),
    col("ingested_at")
).filter(col("customer_id").isNotNull())

# COMMAND ----------

# Step 4: Handle missing values

missing_values = ['\n', '?', '', 'null', 'na', 'n/a']

cleaned_df = (
    cleaned_df
    .withColumn(
        "is_valid",
        when(
            col("customer_name").isNull() |
            col("email").isNull() |
            col("country_code").isNull() |
            col("channel").isNull() |
            lower(trim(col("customer_name"))).isin(missing_values) |
            lower(trim(col("channel"))).isin(missing_values) |
            lower(trim(col("country_code"))).isin(missing_values) |
            lower(trim(col("email"))).isin(missing_values) |
            trim(col("email")).contains("\\N"),   
            0
        ).otherwise(1)
    )
    .withColumn(
        "customer_name",
        when(
            col("customer_name").isNull() |
            lower(trim(col("customer_name"))).isin(missing_values),
            None
        ).otherwise(col("customer_name"))
    )
    .withColumn(
        "email",
        when(
            col("email").isNull() |
            lower(trim(col("email"))).isin(missing_values) |
            trim(col("email")).contains("\\N"),   
            None
        ).otherwise(col("email"))
    )
    .withColumn(
        "channel",
        when(
            col("channel").isNull() |
            lower(trim(col("channel"))).isin(missing_values),
            None
        ).otherwise(col("channel"))
    )
    .withColumn(
        "country_code",
        when(
            col("country_code").isNull() |
            lower(trim(col("country_code"))).isin(missing_values),
            None
        ).otherwise(col("country_code"))
    )
)


# COMMAND ----------

# Step 5: Handle multiple date formats
parsed_date = coalesce(
    expr("try_to_date(registration_date, 'MM/dd/yyyy')"),
    expr("try_to_date(registration_date, 'dd/MM/yyyy')"),
    expr("try_to_date(registration_date, 'yyyy-MM-dd')"),
    expr("try_to_date(registration_date, 'MM-dd-yyyy')"),
    expr("try_to_date(registration_date, 'dd-MM-yyyy')")
)

# Step 6: Apply transformation (yyyy-MM-dd)
transformed_df = (
    cleaned_df
    .withColumn("parsed_date", parsed_date)
    .withColumn(
        "registration_date",
        when(
            col("parsed_date").isNotNull(),
            date_format(col("parsed_date"), "yyyy-MM-dd")
        ).otherwise(col("registration_date"))
    )
    .drop("parsed_date")
)


# COMMAND ----------

# Step 7: Create silver schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.silver")

# Step 8: Write to silver layer
transformed_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("nova_pipeline.silver.silver_customers")

# COMMAND ----------

# Step 9: Display result
display(transformed_df)