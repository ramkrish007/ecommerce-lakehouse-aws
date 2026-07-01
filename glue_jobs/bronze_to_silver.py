"""
AWS Glue Job: Bronze → Silver
Reads raw JSON from S3 bronze, validates, deduplicates, types,
sessionizes clickstream, and writes clean Iceberg tables to silver.
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
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE = f"s3://{args['BRONZE_BUCKET']}"
SILVER = f"s3://{args['SILVER_BUCKET']}"

# Iceberg catalog configuration
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", SILVER)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.write.distribution-mode", "none")

# ===========================
# CLICKSTREAM: bronze → silver
# ===========================
print("Reading clickstream from bronze...")
raw_clicks = spark.read.json(f"{BRONZE}/clickstream/")

VALID_EVENT_TYPES = [
    "page_view", "product_view", "search",
    "add_to_cart", "remove_from_cart", "checkout", "purchase"
]

# Filter bad records
clean_clicks = raw_clicks.filter(
    F.col("event_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("session_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    F.col("event_type").isin(VALID_EVENT_TYPES) &
    (F.col("price") >= 0)
)

# Dedup
clean_clicks = clean_clicks.dropDuplicates(["event_id"])

# Type casting + derived columns
clean_clicks = clean_clicks \
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
    .withColumn("price", F.col("price").cast("decimal(10,2)")) \
    .withColumn("quantity", F.col("quantity").cast("int")) \
    .withColumn("event_date", F.to_date("event_timestamp")) \
    .withColumn("event_hour", F.hour("event_timestamp"))

# Sessionize
session_window = Window.partitionBy("session_id").orderBy("event_timestamp")
clean_clicks = clean_clicks \
    .withColumn("event_seq", F.row_number().over(session_window)) \
    .withColumn("session_start", F.first("event_timestamp").over(
        Window.partitionBy("session_id").orderBy("event_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )) \
    .withColumn("is_purchase", F.when(F.col("event_type") == "purchase", True).otherwise(False))

# Audit columns
silver_clicks = clean_clicks \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("clickstream_kafka")) \
    .withColumn("pipeline_version", F.lit("1.0"))

print(f"Silver clickstream: {silver_clicks.count()} records")

# ===========================
# PRODUCTS: bronze → silver
# ===========================
print("Reading products from bronze...")
raw_products = spark.read.json(f"{BRONZE}/products/")

silver_products = raw_products \
    .dropDuplicates(["product_id"]) \
    .withColumn("base_price", F.col("base_price").cast("decimal(10,2)")) \
    .withColumn("in_stock", F.col("in_stock").cast("boolean")) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("rest_catalog"))

# ===========================
# CUSTOMERS: bronze → silver
# ===========================
print("Reading customers from bronze...")
raw_customers = spark.read.json(f"{BRONZE}/customers/")

silver_customers = raw_customers \
    .dropDuplicates(["customer_id"]) \
    .withColumn("signup_date", F.to_date("signup_date")) \
    .withColumn("is_prime", F.col("is_prime").cast("boolean")) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("rest_catalog"))

# ===========================
# ORDERS: bronze → silver
# ===========================
print("Reading orders from bronze...")
raw_orders = spark.read.json(f"{BRONZE}/orders/")

VALID_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]

silver_orders = raw_orders \
    .filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("status").isin(VALID_STATUSES) &
        (F.col("order_amount") >= 0) &
        (F.col("quantity") >= 1)
    ) \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp")) \
    .withColumn("order_date", F.to_date("order_timestamp")) \
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)")) \
    .withColumn("order_amount", F.col("order_amount").cast("decimal(10,2)")) \
    .withColumn("quantity", F.col("quantity").cast("int")) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("rest_orders"))

# Clickstream — sort by partition column
silver_clicks.orderBy("event_date") \
    .writeTo("glue_catalog.ecom_lakehouse_db.silver_clickstream") \
    .using("iceberg") \
    .partitionedBy("event_date") \
    .createOrReplace()

# Products — no partition, no sort needed
silver_products.writeTo("glue_catalog.ecom_lakehouse_db.silver_products") \
    .using("iceberg") \
    .createOrReplace()

# Customers — no partition, no sort needed
silver_customers.writeTo("glue_catalog.ecom_lakehouse_db.silver_customers") \
    .using("iceberg") \
    .createOrReplace()

# Orders — sort by partition column
silver_orders.orderBy("order_date") \
    .writeTo("glue_catalog.ecom_lakehouse_db.silver_orders") \
    .using("iceberg") \
    .partitionedBy("order_date") \
    .createOrReplace()

print(f"Silver orders: {silver_orders.count()} written!")

print("\n=== Bronze → Silver complete! ===")
job.commit()