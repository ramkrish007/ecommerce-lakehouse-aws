"""
AWS Glue Job: Silver → Gold
Reads clean Iceberg tables from silver, builds the snowflake schema
gold layer: sub-dimensions, dimensions, and fact tables.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Glue boilerplate ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = f"s3://{args['SILVER_BUCKET']}"
GOLD = f"s3://{args['GOLD_BUCKET']}"

# Iceberg catalog configuration
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", GOLD)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.write.distribution-mode", "none")

DB = "glue_catalog.ecom_lakehouse_db"

# ===========================
# Read silver tables
# ===========================
print("Reading silver tables...")
silver_products = spark.read.table(f"{DB}.silver_products")
silver_customers = spark.read.table(f"{DB}.silver_customers")
silver_orders = spark.read.table(f"{DB}.silver_orders")
silver_clicks = spark.read.table(f"{DB}.silver_clickstream")

print(f"  Products: {silver_products.count()}")
print(f"  Customers: {silver_customers.count()}")
print(f"  Orders: {silver_orders.count()}")
print(f"  Clickstream: {silver_clicks.count()}")

# ===========================
# SUB-DIMENSIONS (snowflake)
# ===========================
print("Building sub-dimensions...")

dim_category = silver_products \
    .select("category").distinct() \
    .withColumn("category_id", F.monotonically_increasing_id() + 1) \
    .select("category_id", F.col("category").alias("category_name"))

dim_brand = silver_products \
    .select("brand").distinct() \
    .withColumn("brand_id", F.monotonically_increasing_id() + 1) \
    .select("brand_id", F.col("brand").alias("brand_name"))

dim_city = silver_customers \
    .select("city", "state").distinct() \
    .withColumn("city_id", F.monotonically_increasing_id() + 1) \
    .select("city_id", F.col("city").alias("city_name"), "state")

dim_category.writeTo(f"{DB}.gold_dim_category").using("iceberg").createOrReplace()
dim_brand.writeTo(f"{DB}.gold_dim_brand").using("iceberg").createOrReplace()
dim_city.writeTo(f"{DB}.gold_dim_city").using("iceberg").createOrReplace()
print(f"  dim_category: {dim_category.count()}, dim_brand: {dim_brand.count()}, dim_city: {dim_city.count()}")

# ===========================
# MAIN DIMENSIONS
# ===========================
print("Building main dimensions...")

dim_product = silver_products \
    .join(dim_category, silver_products["category"] == dim_category["category_name"], "left") \
    .join(dim_brand, silver_products["brand"] == dim_brand["brand_name"], "left") \
    .select("product_id", "product_name", "category_id", "brand_id", "base_price", "in_stock")

dim_product.writeTo(f"{DB}.gold_dim_product").using("iceberg").createOrReplace()
print(f"  dim_product: {dim_product.count()}")

dim_customer = silver_customers \
    .join(dim_city,
          (silver_customers["city"] == dim_city["city_name"]) &
          (silver_customers["state"] == dim_city["state"]),
          "left") \
    .select(
        F.col("customer_id"), F.col("name"), F.col("email"),
        F.col("city_id"), F.col("is_prime"), F.col("signup_date"),
        F.current_date().alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit(True).alias("is_current")
    )

dim_customer.writeTo(f"{DB}.gold_dim_customer").using("iceberg").createOrReplace()
print(f"  dim_customer: {dim_customer.count()}")

dim_date = spark.range(60).select(
    F.date_sub(F.current_date(), F.col("id").cast("int")).alias("date_key")
) \
    .withColumn("day_of_week", F.dayofweek("date_key")) \
    .withColumn("day_name", F.date_format("date_key", "EEEE")) \
    .withColumn("day_of_month", F.dayofmonth("date_key")) \
    .withColumn("week_of_year", F.weekofyear("date_key")) \
    .withColumn("month", F.month("date_key")) \
    .withColumn("month_name", F.date_format("date_key", "MMMM")) \
    .withColumn("quarter", F.quarter("date_key")) \
    .withColumn("year", F.year("date_key")) \
    .withColumn("is_weekend", F.when(F.dayofweek("date_key").isin(1, 7), True).otherwise(False))

dim_date.writeTo(f"{DB}.gold_dim_date").using("iceberg").createOrReplace()
print(f"  dim_date: {dim_date.count()}")

# ===========================
# FACT TABLES
# ===========================
print("Building fact tables...")

fact_orders = silver_orders.select(
    "order_id", "customer_id", "product_id",
    F.col("order_date").alias("date_key"),
    "category", "quantity", "unit_price", "order_amount",
    "status", "order_timestamp"
)

fact_orders.orderBy("date_key") \
    .writeTo(f"{DB}.gold_fact_orders").using("iceberg") \
    .partitionedBy("date_key").createOrReplace()
print(f"  fact_orders: {fact_orders.count()}")

fact_sessions = silver_clicks \
    .groupBy("session_id", "user_id", "device", "channel", "event_date") \
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_timestamp").alias("session_start"),
        F.max("event_timestamp").alias("session_end"),
        F.countDistinct("product_id").alias("products_viewed"),
        F.max("is_purchase").alias("has_purchase"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
    ) \
    .withColumn("session_duration_sec",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))

fact_sessions.orderBy("event_date") \
    .writeTo(f"{DB}.gold_fact_sessions").using("iceberg") \
    .partitionedBy("event_date").createOrReplace()
print(f"  fact_sessions: {fact_sessions.count()}")

FUNNEL_STAGES = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]

fact_funnel = silver_clicks \
    .filter(F.col("event_type").isin(FUNNEL_STAGES)) \
    .groupBy("event_date", "event_type") \
    .agg(
        F.countDistinct("session_id").alias("unique_sessions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.count("*").alias("total_events")
    ) \
    .withColumnRenamed("event_type", "funnel_stage")

fact_funnel.orderBy("event_date") \
    .writeTo(f"{DB}.gold_fact_funnel_daily").using("iceberg") \
    .partitionedBy("event_date").createOrReplace()
print(f"  fact_funnel_daily: {fact_funnel.count()}")

print("\n=== Silver → Gold complete! ===")
job.commit()