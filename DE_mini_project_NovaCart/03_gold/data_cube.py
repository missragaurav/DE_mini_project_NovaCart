from pyspark.sql.functions import *

# Step 1: Set catalog
spark.sql("USE CATALOG nova_pipeline")

# Step 2: Read fact table
fact_orders = spark.table("nova_pipeline.gold.fact_orders")

# Step 3: Add date columns
fact_orders = fact_orders \
    .withColumn("order_date_parsed", to_date(col("order_date"))) \
    .withColumn("year", year(col("order_date_parsed"))) \
    .withColumn("month", month(col("order_date_parsed")))

# Step 4: Create Data Cube
cube_df = fact_orders.cube(
    "year",
    "month",
    "country",
    "channel",
    "product_id"
).agg(
    round(sum("revenue_usd"), 2).alias("total_revenue_usd"),
    countDistinct("order_id").alias("total_orders"),
    round(avg("revenue_usd"), 2).alias("avg_order_value")
)

# Step 5: FIX → Cast numeric columns to string
# (IMPORTANT: avoids 'ALL' casting error)
cube_df = cube_df \
    .withColumn("year", col("year").cast("string")) \
    .withColumn("month", col("month").cast("string"))

# Step 6: Replace NULLs with 'ALL'
cube_df = cube_df.fillna({
    "year": "ALL",
    "month": "ALL",
    "country": "ALL",
    "channel": "ALL",
    "product_id": "ALL"
})

# Step 7: Create gold schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS nova_pipeline.gold")

# Step 8: Write to Gold Layer
cube_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nova_pipeline.gold.cube_sales")

# Step 9: Display result
display(cube_df)
