# Databricks notebook source
from pyspark.sql.functions import *

spark.sql("USE CATALOG nova_pipeline")

fact_orders = spark.table("nova_pipeline.gold.fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Total Revenue_**

# COMMAND ----------

kpi_total_revenue = fact_orders.agg(
    round(sum(when(col("status") == "completed", col("revenue_usd"))), 2).alias("total_revenue_usd")
)

display(kpi_total_revenue)

kpi_total_revenue.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_total_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Revenue by Country_**

# COMMAND ----------

kpi_revenue_country = fact_orders.groupBy("country") \
    .agg(round(sum(when(col("status") == "completed", col("revenue_usd"))), 2).alias("total_revenue_usd"))

display(kpi_revenue_country)

kpi_revenue_country.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_revenue_country")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Revenue by Channel_**

# COMMAND ----------

kpi_revenue_channel = fact_orders.groupBy("channel") \
    .agg(round(sum(when(col("status") == "completed", col("revenue_usd"))), 2).alias("revenue"))

display(kpi_revenue_channel)

kpi_revenue_channel.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_revenue_channel")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Completed Orders Count_**

# COMMAND ----------

kpi_completed_orders = fact_orders \
    .filter(col("status") == "completed") \
    .agg(countDistinct("order_id").alias("completed_orders"))

display(kpi_completed_orders)

kpi_completed_orders.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_completed_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Completed Order Rate_**

# COMMAND ----------

kpi_completed_order_rate = fact_orders.agg((
    countDistinct(when(col("status") == "completed", col("order_id"))) /
    countDistinct("order_id")
).alias("completed_order_rate"))

display(kpi_completed_order_rate)

kpi_completed_order_rate.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_completed_order_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC **_AOV (Average Order Value)_**

# COMMAND ----------

kpi_aov = fact_orders.groupBy("order_id") \
    .agg(round(sum(when(col("status") == "completed", col("revenue_usd"))), 2).alias("total_revenue_usd")) \
    .agg(round(avg("total_revenue_usd"), 2).alias("AOV"))

display(kpi_aov)

kpi_aov.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_aov")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Top 5 Products_**

# COMMAND ----------

kpi_top_products = fact_orders \
    .groupBy("product_id") \
    .agg(round(sum(when(col("status") == "completed", col("revenue_usd"))), 2).alias("revenue_usd")) \
    .orderBy(desc("revenue_usd")) \
    .limit(5)

display(kpi_top_products)

kpi_top_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_top_products")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Active Customers_**

# COMMAND ----------

kpi_active_customers = fact_orders \
    .select(countDistinct("customer_id").alias("active_customers"))

display(kpi_active_customers)

kpi_active_customers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_active_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Customer Acquisition_**

# COMMAND ----------

kpi_customer_acquisition = fact_orders \
    .groupBy("customer_id") \
    .agg(min("order_date").alias("first_order_date")) \
    .select(
        date_format(to_date(col("first_order_date"), "yyyy-MM-dd"), "yyyy-MM").alias("year_month"),
        col("customer_id")
    ) \
    .groupBy("year_month") \
    .agg(countDistinct("customer_id").alias("new_customers")) \
    .orderBy("year_month")

display(kpi_customer_acquisition)

kpi_customer_acquisition.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_customer_acquisition")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Data Quality Score_**

# COMMAND ----------

kpi_data_quality = fact_orders.select((
    sum(when(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("status").isNotNull(),
        1
    ).otherwise(0)) / count("*")
).alias("data_quality_score"))

display(kpi_data_quality)

kpi_data_quality.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.kpi_data_quality")