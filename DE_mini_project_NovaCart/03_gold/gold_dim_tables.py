# Databricks notebook source
from pyspark.sql.functions import *

# Step 1: Set catalog
spark.sql("USE CATALOG nova_pipeline")

# Step 2: Read silver tables
silver_orders = spark.table("nova_pipeline.silver.silver_orders")
silver_customers = spark.table("nova_pipeline.silver.silver_customers")
silver_products = spark.table("nova_pipeline.silver.silver_products")
silver_order_items = spark.table("nova_pipeline.silver.silver_order_items")
silver_exchange_rates = spark.table("nova_pipeline.silver.silver_exchange_rates")

# COMMAND ----------

# Step 3: Create Dimension Tables
# dim_customers
dim_customers = silver_customers.select(
    "customer_id",
    "customer_name",
    "email",
    "country_code"
).dropDuplicates()

# dim_products
dim_products = silver_products.select(
    "product_id",
    "product_name",
    "category",
    "price",
    "country_code",
    "currency"
).dropDuplicates()

# dim_date (your format is already yyyy-MM-dd)
dim_date = silver_orders.select(
    to_date(col("order_date"), "yyyy-MM-dd").alias("date")
).dropDuplicates() \
.withColumn("year", year("date")) \
.withColumn("month", month("date")) \
.withColumn("day", dayofmonth("date"))

# Step 4: Create gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.gold")

# Step 5: Write gold tables
dim_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.dim_customers")

dim_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.dim_products")

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.dim_date")


# COMMAND ----------

# Step 6: Display
display(dim_customers)
display(dim_products)
display(dim_date)