# Databricks notebook source
from pyspark.sql.functions import *

# Step 1: Set catalog & schema

spark.sql("USE CATALOG nova_pipeline")
spark.sql("USE SCHEMA bronze")

# Step 2: Read from bronze table
df = spark.table("orders_bronze")

# COMMAND ----------

# Step 3: Select required columns
cleaned_df = df.select(
    col("order_id"),
    col("customer_id"),
    col("order_date"),
    col("order_status"),
    col("country_code"),
    col("channel"),
    col("total_amount"),
    col("currency"),
    col("_modified"),
    col("ingested_at")   
).filter(col("order_id").isNotNull())

# COMMAND ----------

# Step 4: Handle missing values
missing_values = ['\n', '?', '', 'null', 'na', 'n/a']

cleaned_df = (
    cleaned_df
    .withColumn(
        "is_valid",
        when(
            col("customer_id").isNull() |
            col("country_code").isNull() |
            col("channel").isNull() |
            lower(trim(col("customer_id"))).isin(missing_values) |
            lower(trim(col("channel"))).isin(missing_values) |
            lower(trim(col("country_code"))).isin(missing_values) |
            trim(col("customer_id")).contains("\\N") |
            trim(col("channel")).contains("\\N") |
            trim(col("country_code")).contains("\\N"),
            0
        ).otherwise(1)
    )
    .withColumn(
        "customer_id",
        when(
            col("customer_id").isNull() |
            lower(trim(col("customer_id"))).isin(missing_values) |
            trim(col("customer_id")).contains("\\N"),
            None
        ).otherwise(col("customer_id"))
    )
    .withColumn(
        "channel",
        when(
            col("channel").isNull() |
            lower(trim(col("channel"))).isin(missing_values) |
            trim(col("channel")).contains("\\N"),
            None
        ).otherwise(col("channel"))
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

# Step 5: Date transformation
parsed_date = coalesce(
    expr("try_to_date(order_date, 'MM/dd/yyyy')"),
    expr("try_to_date(order_date, 'dd/MM/yyyy')"),
    expr("try_to_date(order_date, 'yyyy-MM-dd')"),
    expr("try_to_date(order_date, 'MM-dd-yyyy')"),
    expr("try_to_date(order_date, 'dd-MM-yyyy')")
)

transformed_df = (
    cleaned_df
    .withColumn("parsed_date", parsed_date)
    .withColumn(
        "order_date",
        when(
            col("parsed_date").isNotNull(),
            date_format(col("parsed_date"), "yyyy-MM-dd")   
        ).otherwise(col("order_date"))
    )
    .drop("parsed_date")
)

# COMMAND ----------

# Step 6: Status mapping
status_map = spark.createDataFrame([
    ("completed", "completed"), ("completado", "completed"),
    ("abgeschlossen", "completed"), ("पूर्ण", "completed"), ("已完成", "completed"),

    ("pending", "pending"), ("pendiente", "pending"),
    ("ausstehend", "pending"), ("लंबित", "pending"), ("待处理", "pending"),

    ("cancelled", "cancelled"), ("cancelado", "cancelled"),
    ("storniert", "cancelled"), ("रद्द", "cancelled"), ("已取消", "cancelled"),

    ("shipped", "shipped"), ("versandt", "shipped"),
    ("enviado", "shipped"), ("भेज दिया", "shipped"), ("已发货", "shipped"),
], ["raw_status", "standard_status"])

standardised_df = transformed_df.join(
    status_map,
    col("order_status") == col("raw_status"),
    "left"
)


# COMMAND ----------

# Step 7: Clean status column
standardised_df = (
    standardised_df
    .withColumn(
        "is_invalid_status",
        when(
            col("order_status").isNull() |
            lower(trim(col("order_status"))).isin(missing_values),
            1
        ).otherwise(0)
    )
    .withColumn(
        "order_status",
        when(
            col("order_status").isNull() |
            lower(trim(col("order_status"))).isin(missing_values),
            None
        ).otherwise(col("order_status"))
    )
    .withColumn(
        "standard_status",
        when(
            col("standard_status").isNull() |
            lower(trim(col("standard_status"))).isin(missing_values),
            None
        ).otherwise(col("standard_status"))
    )
    .drop("raw_status")
)

# COMMAND ----------

# Step 8: Write to silver
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.silver")

standardised_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.silver.silver_orders")  

# Step 9: Display
display(standardised_df)