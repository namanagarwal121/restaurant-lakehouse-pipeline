import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml

EH_NAMESPACE = "eh-ns-dbxproject-az"
EH_NAME      = "orders"

EH_CONN_STR  = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
  "kafka.request.timeout.ms" : 100000,
  "kafka.session.timeout.ms" : 100000,
  "maxOffsetsPerTrigger"     : 100000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}

@dlt.table(name="orders_streaming")
def orders_raw():
  df = spark.readStream.format("kafka") \
      .options(**KAFKA_OPTIONS) \
      .load()

  df = df.selectExpr("CAST(value AS STRING)")\
        .withColumn('ingestion_time' , current_timestamp())

  return df