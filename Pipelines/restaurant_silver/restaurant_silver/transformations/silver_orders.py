import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_type", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType()),
    StructField("created_at", StringType()),
    StructField("items", ArrayType(
        StructType([
            StructField("item_id", StringType()),
            StructField("name", StringType()),
            StructField("category", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("subtotal", DoubleType())
        ])
    ))
])

items_schema = ArrayType(
    StructType([
        StructField("item_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("subtotal", DoubleType())
    ])
)

dlt.create_streaming_table(
    name="silver_orders_raw",
    comment="Raw orders from both streaming and historical sources"
)

@dlt.append_flow(
    target="silver_orders_raw",
    name="streaming_orders_flow",
    comment="Continuous flow for streaming orders"
)
def streaming_orders_flow():
    df = spark.readStream.table("workspace.bronze.orders_streaming")

    df = df.select(
        from_json(col("value"), order_schema).alias("data"),
        col("ingestion_time")
    ).select("data.*", "ingestion_time")

    df = df \
        .withColumn("created_at", to_timestamp("created_at")) \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumn("source", lit("streaming"))

    return df

@dlt.append_flow(
    target="silver_orders_raw",
    name="historical_orders_flow",
    once=True,
    comment="One-time backfill for historical orders"
)
def historical_orders_flow():
    df = spark.read.table("workspace.bronze.historical_orders")

    df = df.select(
        col("order_id").cast("string"),
        col("timestamp").cast("string"),
        col("restaurant_id").cast("string"),
        col("customer_id").cast("string"),
        col("order_type").cast("string"),
        col("total_amount").cast("double"),
        col("payment_method").cast("string"),
        col("order_status").cast("string"),
        col("created_at").cast("string"),
        from_json(col("items"), items_schema).alias("items"),
        col("ingestion_time")
    )

    df = df \
        .withColumn("created_at", to_timestamp("created_at")) \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .withColumn("source", lit("historical"))

    return df

dlt.create_streaming_table(
    name="silver_orders",
    comment="Deduplicated orders - latest record per order_id",
    expect_all={
        "valid_order_id": "order_id IS NOT NULL",
        "valid_amount": "total_amount >= 0"
    }
)

dlt.create_auto_cdc_flow(
    target="silver_orders",
    source="silver_orders_raw",
    keys=["order_id"],
    sequence_by="created_at",
    stored_as_scd_type=1
)
