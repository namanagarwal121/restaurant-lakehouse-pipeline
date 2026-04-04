import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="fact_order_items",
    comment="Exploded order items from orders"
)
def fact_order_items():

    df = dlt.read("silver_orders")

    df = df.withColumn("item", explode("items"))

    return df.select(
        col("order_id"),
        col("customer_id"),
        col("restaurant_id"),
        col("created_at"),
        col("item.item_id").alias("item_id"),
        col("item.name").alias("item_name"),
        col("item.category"),
        col("item.quantity"),
        col("item.unit_price"),
        col("item.subtotal")
    )

@dlt.table(
    name="dim_customers",
    comment="Customer dimension"
)
def dim_customers():

    df = spark.read.table("workspace.bronze.customers")

    return df.select(
        col("customer_id"),
        col("name").alias("customer_name"),
        col("email"),
        col("phone").cast("string"),
        col("city"),
        col("join_date"),
        col("ingestion_time")
    )

@dlt.table(
    name="dim_restaurants",
    comment="Restaurant dimension"
)
def dim_restaurants():

    df = spark.read.table("workspace.bronze.restaurants")

    return df.select(
        col("restaurant_id"),
        col("name").alias("restaurant_name"),
        col("city"),
        col("country"),
        col("address"),
        col("opening_date"),
        col("phone"),
        col("ingestion_time")
    )

@dlt.table(
    name="dim_menu_items",
    comment="Menu items dimension"
)
def dim_menu_items():

    df = spark.read.table("workspace.bronze.menu_items")

    return df.select(
        col("item_id"),
        col("restaurant_id"),
        col("name").alias("item_name"),
        col("category"),
        col("price"),
        col("ingredients"),
        col("is_vegetarian"),
        col("spice_level"),
        col("ingestion_time")
    )

@dlt.table(
    name="fact_reviews",
    comment="Customer reviews fact table"
)
def fact_reviews():

    df = spark.read.table("workspace.bronze.customer_reviews")

    return df.select(
        col("review_id"),
        col("order_id"),
        col("customer_id"),
        col("restaurant_id"),
        col("review_text"),
        col("rating").cast("int"),
        col("review_timestamp"),
        col("ingestion_time")
    )