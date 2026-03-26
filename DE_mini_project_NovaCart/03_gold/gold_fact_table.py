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

# Step 3: Read gold dimension tables
dim_customers = spark.table("nova_pipeline.gold.dim_customers")
dim_products = spark.table("nova_pipeline.gold.dim_products")
dim_date = spark.table("nova_pipeline.gold.dim_date")

# Step 4: Create FACT table
fact_orders = silver_order_items \
    .join(silver_orders, "order_id", "left") \
    .join(dim_products, "product_id", "left") \
    .join(dim_customers, "customer_id", "left") \
    .join(
        silver_exchange_rates,
        silver_exchange_rates.currency_code == dim_products.currency,
        "left"
    ) \
    .select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("order_date"),
        col("standard_status").alias("status"),
        silver_orders.country_code.alias("country"),
        silver_orders.channel,
        col("quantity"),
        col("unit_price").alias("local_price"),
        round(col("unit_price") * col("exchange_rate_to_usd"), 2).alias("price_usd"),
        round(col("quantity") * col("unit_price") * col("exchange_rate_to_usd"), 2).alias("revenue_usd")
    )


# COMMAND ----------

# Step 5: Display
display(fact_orders)

# COMMAND ----------

# Step 6: Write to gold table
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.gold")

fact_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.fact_orders")