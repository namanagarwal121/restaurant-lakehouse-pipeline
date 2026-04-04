import dlt
from pyspark.sql.functions import *

dlt.create_streaming_table(name="d_sales_summary")

@dlt.materialized_view
def sales_summary_source():

    orders = spark.read.table("workspace.silver.silver_orders")
    restaurants = spark.read.table("workspace.silver.dim_restaurants")

    df = (
        orders
        .withColumn("order_date", to_date("created_at"))
        .groupBy("restaurant_id", "order_date")
        .agg(
            count("*").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            max("created_at").alias("last_updated")
        )
    )

    return df.join(restaurants, "restaurant_id", "left")

dlt.create_auto_cdc_from_snapshot_flow(
    target="d_sales_summary",
    source="sales_summary_source",
    keys=["restaurant_id", "order_date"],
    stored_as_scd_type=2
)

dlt.create_streaming_table(name="d_customer_360")

@dlt.materialized_view
def customer_360_source():

    orders = spark.read.table("workspace.silver.silver_orders")
    customers = spark.read.table("workspace.silver.dim_customers")

    df = (
        orders
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("avg_order_value"),
            max("created_at").alias("last_order_date"),
            max("created_at").alias("last_updated")
        )
    )

    return df.join(customers, "customer_id", "left")

dlt.create_auto_cdc_from_snapshot_flow(
    target="d_customer_360",
    source="customer_360_source",
    keys=["customer_id"],
    stored_as_scd_type=2
)

dlt.create_streaming_table(name="d_restaurant_reviews")

@dlt.materialized_view
def restaurant_reviews_source():

    reviews = spark.read.table("workspace.silver.fact_reviews")
    restaurants = spark.read.table("workspace.silver.dim_restaurants")

    df = (
        reviews
        .groupBy("restaurant_id")
        .agg(
            count("*").alias("total_reviews"),
            avg("rating").alias("avg_rating"),
            max("review_timestamp").alias("last_updated")
        )
    )

    return df.join(restaurants, "restaurant_id", "left")

dlt.create_auto_cdc_from_snapshot_flow(
    target="d_restaurant_reviews",
    source="restaurant_reviews_source",
    keys=["restaurant_id"],
    stored_as_scd_type=2
)
