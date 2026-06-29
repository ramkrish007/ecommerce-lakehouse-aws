# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VOLUME = "/Volumes/workspace/default/ecom_bronze_sample"

raw_products  = spark.read.json(f"{VOLUME}/sample_products.json")
raw_customers = spark.read.json(f"{VOLUME}/sample_customers.json")
raw_orders    = spark.read.json(f"{VOLUME}/sample_orders.json")

print(f"Raw products:  {raw_products.count()}")
print(f"Raw customers: {raw_customers.count()}")
print(f"Raw orders:    {raw_orders.count()}")

# COMMAND ----------

# Products: no corruption was injected, so just type-cast and standardize
silver_products = raw_products \
    .dropDuplicates(["product_id"]) \
    .withColumn("base_price", F.col("base_price").cast("decimal(10,2)")) \
    .withColumn("in_stock", F.col("in_stock").cast("boolean")) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("rest_catalog"))

print(f"Silver products: {silver_products.count()}")
silver_products.printSchema()
silver_products.show(5, truncate=False)

# COMMAND ----------

# Customers: no corruption injected, type-cast and standardize
silver_customers = raw_customers \
    .dropDuplicates(["customer_id"]) \
    .withColumn("signup_date", F.to_date("signup_date")) \
    .withColumn("is_prime", F.col("is_prime").cast("boolean")) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("rest_catalog"))

print(f"Silver customers: {silver_customers.count()}")
silver_customers.printSchema()
silver_customers.show(5, truncate=False)

# COMMAND ----------

# Orders: ~5% corruption was injected — filter bad records
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

bad_orders = raw_orders.count() - silver_orders.count()
print(f"Silver orders: {silver_orders.count()}, Removed: {bad_orders}")
silver_orders.printSchema()
silver_orders.show(5, truncate=False)

# COMMAND ----------

# Write silver products
silver_products.writeTo("workspace.default.silver_products") \
    .using("iceberg") \
    .createOrReplace()
print("Silver products written!")

# Write silver customers
silver_customers.writeTo("workspace.default.silver_customers") \
    .using("iceberg") \
    .createOrReplace()
print("Silver customers written!")

# Write silver orders (partitioned by order_date)
silver_orders.writeTo("workspace.default.silver_orders") \
    .using("iceberg") \
    .partitionedBy("order_date") \
    .createOrReplace()
print("Silver orders written!")

# COMMAND ----------

# Cell 6 — Sub-dimensions: dim_category, dim_brand, dim_city
# These are the normalized lookup tables that make this a snowflake, not a star

# --- dim_category ---
dim_category = silver_products \
    .select("category") \
    .distinct() \
    .withColumn("category_id", F.monotonically_increasing_id() + 1) \
    .select("category_id", F.col("category").alias("category_name"))

print("dim_category:")
dim_category.show(truncate=False)

# --- dim_brand ---
dim_brand = silver_products \
    .select("brand") \
    .distinct() \
    .withColumn("brand_id", F.monotonically_increasing_id() + 1) \
    .select("brand_id", F.col("brand").alias("brand_name"))

print("dim_brand:")
dim_brand.show(truncate=False)

# --- dim_city ---
dim_city = silver_customers \
    .select("city", "state") \
    .distinct() \
    .withColumn("city_id", F.monotonically_increasing_id() + 1) \
    .select("city_id", F.col("city").alias("city_name"), "state")

print(f"dim_city: {dim_city.count()} unique cities")
dim_city.show(10, truncate=False)

# COMMAND ----------

# Cell 7 — dim_product: enriched with category_id and brand_id from sub-dimensions
dim_product = silver_products \
    .join(dim_category, silver_products["category"] == dim_category["category_name"], "left") \
    .join(dim_brand, silver_products["brand"] == dim_brand["brand_name"], "left") \
    .select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category_id"),      # FK to dim_category (not the text)
        F.col("brand_id"),          # FK to dim_brand (not the text)
        F.col("base_price"),
        F.col("in_stock")
    )

print(f"dim_product: {dim_product.count()} rows")
dim_product.show(5, truncate=False)

# COMMAND ----------

# Cell 8 — dim_customer: normalized with city_id FK instead of city/state text
dim_customer = silver_customers \
    .join(dim_city, 
          (silver_customers["city"] == dim_city["city_name"]) & 
          (silver_customers["state"] == dim_city["state"]), 
          "left") \
    .select(
        F.col("customer_id"),
        F.col("name"),
        F.col("email"),
        F.col("city_id"),           # FK to dim_city (not city/state text)
        F.col("is_prime"),
        F.col("signup_date"),
        # SCD2 columns — for tracking changes over time
        F.current_date().alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit(True).alias("is_current")
    )

print(f"dim_customer: {dim_customer.count()} rows")
dim_customer.show(5, truncate=False)

# COMMAND ----------

# Cell 9 — dim_date: a date dimension table covering the date range in orders
from pyspark.sql.types import DateType
from datetime import timedelta

# Generate a date range covering 60 days back from today
date_range = spark.range(60).select(
    F.date_sub(F.current_date(), F.col("id").cast("int")).alias("date_key")
)

dim_date = date_range \
    .withColumn("day_of_week", F.dayofweek("date_key")) \
    .withColumn("day_name", F.date_format("date_key", "EEEE")) \
    .withColumn("day_of_month", F.dayofmonth("date_key")) \
    .withColumn("week_of_year", F.weekofyear("date_key")) \
    .withColumn("month", F.month("date_key")) \
    .withColumn("month_name", F.date_format("date_key", "MMMM")) \
    .withColumn("quarter", F.quarter("date_key")) \
    .withColumn("year", F.year("date_key")) \
    .withColumn("is_weekend", F.when(F.dayofweek("date_key").isin(1, 7), True).otherwise(False)) \
    .orderBy("date_key")

print(f"dim_date: {dim_date.count()} rows")
dim_date.show(10, truncate=False)

# COMMAND ----------

# Cell 10 — fact_orders: the central fact table
fact_orders = silver_orders \
    .select(
        F.col("order_id"),
        F.col("customer_id"),       # FK to dim_customer
        F.col("product_id"),        # FK to dim_product
        F.col("order_date").alias("date_key"),  # FK to dim_date
        F.col("category"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("order_amount"),
        F.col("status"),
        F.col("order_timestamp")
    )

print(f"fact_orders: {fact_orders.count()} rows")
fact_orders.show(5, truncate=False)

# Quick GMV check
fact_orders.select(
    F.sum("order_amount").alias("total_gmv"),
    F.count("*").alias("total_orders"),
    F.avg("order_amount").alias("avg_order_value")
).show()

# COMMAND ----------

# Cell 11 — fact_sessions: one row per session, derived from silver clickstream
silver_clicks = spark.read.table("workspace.default.silver_clickstream")

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
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
    )

print(f"fact_sessions: {fact_sessions.count()} rows")
fact_sessions.show(5, truncate=False)

# COMMAND ----------

# Cell 12 — fact_funnel_daily: daily counts per funnel stage
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

print("fact_funnel_daily:")
fact_funnel.orderBy("event_date", "funnel_stage").show(truncate=False)

# COMMAND ----------

# Cell 13 — Write all gold tables

# Sub-dimensions
dim_category.writeTo("workspace.default.gold_dim_category").using("iceberg").createOrReplace()
dim_brand.writeTo("workspace.default.gold_dim_brand").using("iceberg").createOrReplace()
dim_city.writeTo("workspace.default.gold_dim_city").using("iceberg").createOrReplace()
print("Sub-dimensions written!")

# Main dimensions
dim_product.writeTo("workspace.default.gold_dim_product").using("iceberg").createOrReplace()
dim_customer.writeTo("workspace.default.gold_dim_customer").using("iceberg").createOrReplace()
dim_date.writeTo("workspace.default.gold_dim_date").using("iceberg").createOrReplace()
print("Main dimensions written!")

# Facts
fact_orders.writeTo("workspace.default.gold_fact_orders").using("iceberg") \
    .partitionedBy("date_key").createOrReplace()
fact_sessions.writeTo("workspace.default.gold_fact_sessions").using("iceberg") \
    .partitionedBy("event_date").createOrReplace()
fact_funnel.writeTo("workspace.default.gold_fact_funnel_daily").using("iceberg") \
    .partitionedBy("event_date").createOrReplace()
print("Fact tables written!")

print("\nGold layer complete!")

# COMMAND ----------

# Cell 14 — Quick verification of the entire gold layer
gold_tables = [
    "gold_dim_category", "gold_dim_brand", "gold_dim_city",
    "gold_dim_product", "gold_dim_customer", "gold_dim_date",
    "gold_fact_orders", "gold_fact_sessions", "gold_fact_funnel_daily"
]

for table in gold_tables:
    count = spark.read.table(f"workspace.default.{table}").count()
    print(f"  {table}: {count} rows")

# COMMAND ----------

# Cell 15 — Test the snowflake joins: daily GMV by category
spark.sql("""
    SELECT 
        d.date_key,
        c.category_name,
        SUM(f.order_amount) AS daily_gmv,
        COUNT(*) AS order_count
    FROM workspace.default.gold_fact_orders f
    JOIN workspace.default.gold_dim_product p ON f.product_id = p.product_id
    JOIN workspace.default.gold_dim_category c ON p.category_id = c.category_id
    JOIN workspace.default.gold_dim_date d ON f.date_key = d.date_key
    GROUP BY d.date_key, c.category_name
    ORDER BY d.date_key, daily_gmv DESC
""").show(20, truncate=False)

# COMMAND ----------

# Cell 16 — Simulate changed customer data (as if a new daily pull arrived)
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType

changed_customers_data = [
    # customer_id, name, email, city, state, signup_date, is_prime
    ("user_1",  "Changed User 1",  "user1@email.com",  "Mumbai",    "Maharashtra",  "2024-05-10", True),   # was some other city
    ("user_10", "Changed User 10", "user10@email.com", "Chennai",   "Tamil Nadu",   "2023-08-15", False),  # was prime=True
    ("user_50", "Changed User 50", "user50@email.com", "Bengaluru", "Karnataka",    "2025-01-20", True),   # city changed
]

schema = StructType([
    StructField("customer_id", StringType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("signup_date", StringType()),
    StructField("is_prime", BooleanType()),
])

new_customers = spark.createDataFrame(changed_customers_data, schema) \
    .withColumn("signup_date", F.to_date("signup_date"))

print("Simulated changed customers:")
new_customers.show(truncate=False)

# COMMAND ----------

# Cell 17 — Check current state of these customers in dim_customer
current = spark.read.table("workspace.default.gold_dim_customer")

print("Current state of the 3 customers (before SCD2 merge):")
current.filter(F.col("customer_id").isin("user_1", "user_10", "user_50")) \
    .select("customer_id", "name", "city_id", "is_prime", "effective_date", "end_date", "is_current") \
    .show(truncate=False)

# COMMAND ----------

# Cell 18 — SCD Type 2 merge: expire old rows, insert new versions

# First, enrich new customers with city_id from dim_city
dim_city_current = spark.read.table("workspace.default.gold_dim_city")

new_enriched = new_customers \
    .join(dim_city_current,
          (new_customers["city"] == dim_city_current["city_name"]) &
          (new_customers["state"] == dim_city_current["state"]),
          "left") \
    .select(
        new_customers["customer_id"],
        new_customers["name"],
        new_customers["email"],
        dim_city_current["city_id"],
        new_customers["is_prime"],
        new_customers["signup_date"],
    )

# Create a temp view for the SQL MERGE
new_enriched.createOrReplaceTempView("new_customer_data")

# The SCD2 MERGE:
#   - When a matching current row has changed values → expire it (set end_date, is_current=false)
#   - Then insert the new version as a fresh current row
spark.sql("""
    MERGE INTO workspace.default.gold_dim_customer AS target
    USING (
        SELECT 
            n.customer_id,
            n.name,
            n.email,
            n.city_id,
            n.is_prime,
            n.signup_date
        FROM new_customer_data n
    ) AS source
    ON target.customer_id = source.customer_id AND target.is_current = true
    WHEN MATCHED AND (
        target.city_id != source.city_id OR
        target.is_prime != source.is_prime OR
        target.name != source.name
    )
    THEN UPDATE SET
        target.end_date = current_date(),
        target.is_current = false
    WHEN NOT MATCHED
    THEN INSERT (customer_id, name, email, city_id, is_prime, signup_date, effective_date, end_date, is_current)
    VALUES (source.customer_id, source.name, source.email, source.city_id, source.is_prime, source.signup_date, current_date(), NULL, true)
""")

print("SCD2 merge complete!")

# COMMAND ----------

# Cell 19 — Insert the new current rows for the changed customers
# (MERGE expired the old ones, now we add the updated versions)

spark.sql("""
    INSERT INTO workspace.default.gold_dim_customer
    SELECT 
        n.customer_id,
        n.name,
        n.email,
        n.city_id,
        n.is_prime,
        n.signup_date,
        current_date() AS effective_date,
        NULL AS end_date,
        true AS is_current
    FROM new_customer_data n
    WHERE n.customer_id IN (
        SELECT customer_id 
        FROM workspace.default.gold_dim_customer 
        WHERE is_current = false AND end_date = current_date()
    )
""")

print("New current rows inserted!")

# COMMAND ----------

# Cell 20 — Verify: the 3 customers should now have 2 rows each (old + new)
result = spark.read.table("workspace.default.gold_dim_customer")

print(f"Total dim_customer rows: {result.count()} (was 5000, should be 5003)")

print("\nSCD2 history for changed customers:")
result.filter(F.col("customer_id").isin("user_1", "user_10", "user_50")) \
    .select("customer_id", "name", "city_id", "is_prime",
            "effective_date", "end_date", "is_current") \
    .orderBy("customer_id", "effective_date") \
    .show(10, truncate=False)

# COMMAND ----------

# Cell 21 — Fix missing cities and NULL city_ids

# Step 1: Register the RAW new_customers (which HAS city/state columns) as a temp view
new_customers.createOrReplaceTempView("new_customers_raw")

# Step 2: Find and insert missing cities
existing_cities = spark.read.table("workspace.default.gold_dim_city")
max_city_id = existing_cities.select(F.max("city_id")).collect()[0][0] or 0

missing_cities = new_customers \
    .join(existing_cities,
          (new_customers["city"] == existing_cities["city_name"]) &
          (new_customers["state"] == existing_cities["state"]),
          "left_anti") \
    .select("city", "state") \
    .distinct()

missing_count = missing_cities.count()
print(f"Missing cities to add: {missing_count}")

if missing_count > 0:
    # Collect and rebuild with proper IDs to avoid lazy eval issues
    missing_list = missing_cities.collect()
    new_city_rows = [(max_city_id + i + 1, row["city"], row["state"]) 
                     for i, row in enumerate(missing_list)]
    
    new_cities_df = spark.createDataFrame(new_city_rows, ["city_id", "city_name", "state"])
    new_cities_df.writeTo("workspace.default.gold_dim_city").append()
    print("Added cities:")
    new_cities_df.show()

# Step 3: Now fix NULL city_ids using the raw view that has city/state
spark.sql("""
    MERGE INTO workspace.default.gold_dim_customer AS target
    USING (
        SELECT r.customer_id, ct.city_id
        FROM new_customers_raw r
        JOIN workspace.default.gold_dim_city ct
            ON r.city = ct.city_name AND r.state = ct.state
    ) AS source
    ON target.customer_id = source.customer_id 
        AND target.is_current = true 
        AND target.city_id IS NULL
    WHEN MATCHED
    THEN UPDATE SET target.city_id = source.city_id
""")

print("\nNULL city_ids fixed!")

# Verify
spark.read.table("workspace.default.gold_dim_customer") \
    .filter(F.col("customer_id").isin("user_1", "user_10", "user_50")) \
    .select("customer_id", "name", "city_id", "is_prime", 
            "effective_date", "end_date", "is_current") \
    .orderBy("customer_id", "effective_date") \
    .show(10, truncate=False)