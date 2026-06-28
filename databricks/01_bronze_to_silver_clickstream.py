# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze → Silver: Clickstream Transformation
# MAGIC
# MAGIC Reads raw clickstream JSON from bronze, applies validation, deduplication,
# MAGIC type casting, sessionization, and audit columns. Writes clean data as an
# MAGIC Iceberg-compatible table partitioned by event_date.
# MAGIC
# MAGIC **Source:** Kafka clickstream events landed in S3 bronze
# MAGIC **Target:** workspace.default.silver_clickstream (Iceberg via UniForm)

# COMMAND ----------

# Cell 1 — Read raw bronze clickstream
SAMPLE_PATH = "/Volumes/workspace/default/ecom_bronze_sample/events_20260625_163546_526.json"

raw_df = spark.read.json(SAMPLE_PATH)

print(f"Raw record count: {raw_df.count()}")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# COMMAND ----------

# Cell 2 — Remove invalid records (same rules as the Lambda validator)
from pyspark.sql import functions as F

VALID_EVENT_TYPES = [
    "page_view", "product_view", "search",
    "add_to_cart", "remove_from_cart", "checkout", "purchase"
]

clean_df = raw_df.filter(
    F.col("event_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("session_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    F.col("event_type").isin(VALID_EVENT_TYPES) &
    (F.col("price") >= 0)
)

bad_count = raw_df.count() - clean_df.count()
print(f"Clean: {clean_df.count()}, Removed: {bad_count}")

# COMMAND ----------

# Cell 3 — Remove duplicate events (same event_id = duplicate delivery)
deduped_df = clean_df.dropDuplicates(["event_id"])

print(f"Before dedup: {clean_df.count()}, After dedup: {deduped_df.count()}")

# COMMAND ----------

# Cell 4 — Proper types + useful derived columns
typed_df = deduped_df \
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
    .withColumn("price", F.col("price").cast("decimal(10,2)")) \
    .withColumn("quantity", F.col("quantity").cast("int")) \
    .withColumn("event_date", F.to_date("event_timestamp")) \
    .withColumn("event_hour", F.hour("event_timestamp"))

typed_df.printSchema()
typed_df.show(5, truncate=False)

# COMMAND ----------

# Cell 5 — Sessionize: order events within each session + derive session metrics
from pyspark.sql.window import Window

session_window = Window.partitionBy("session_id").orderBy("event_timestamp")

silver_df = typed_df \
    .withColumn("event_seq", F.row_number().over(session_window)) \
    .withColumn("session_start", F.first("event_timestamp").over(
        Window.partitionBy("session_id").orderBy("event_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )) \
    .withColumn("is_purchase", F.when(F.col("event_type") == "purchase", True).otherwise(False))

print(f"Silver record count: {silver_df.count()}")
silver_df.select(
    "session_id", "event_seq", "event_type", "event_timestamp",
    "session_start", "is_purchase", "product_id", "price"
).orderBy("session_id", "event_seq").show(15, truncate=False)

# COMMAND ----------

# Cell 6 — Audit columns: track when and how this record was processed
silver_final = silver_df \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("clickstream_kafka")) \
    .withColumn("pipeline_version", F.lit("1.0"))

silver_final.printSchema()
print(f"Final silver count: {silver_final.count()}")

# COMMAND ----------

# Cell 7 — Write to silver as an Iceberg table, partitioned by event_date
silver_final.writeTo("workspace.default.silver_clickstream") \
    .using("iceberg") \
    .partitionedBy("event_date") \
    .createOrReplace()

print("Silver clickstream table written successfully!")

# COMMAND ----------

# Cell 8 — Verify: read back from the Iceberg table
result = spark.read.table("workspace.default.silver_clickstream")
print(f"Table row count: {result.count()}")
result.show(5, truncate=False)

# Check the partitions
spark.sql("""
    SELECT event_date, COUNT(*) as records
    FROM workspace.default.silver_clickstream
    GROUP BY event_date
""").show()

# COMMAND ----------

# Cell 9 — Check table history (time travel versions)
spark.sql("DESCRIBE HISTORY workspace.default.silver_clickstream").show(truncate=False)