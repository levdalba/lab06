# spark_kafka_foreachbatch_avro.py
"""
Spark Streaming consumer for Kafka topic using Avro deserialization.

This script reads Avro-serialized messages from Kafka and writes them to Parquet files
using the foreachBatch method.

Requirements:
- pyspark
- fastavro

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_kafka_foreachbatch_avro.py
"""

import io
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import Row
import fastavro

# Ensure local "data" directory exists
os.makedirs("data", exist_ok=True)

# Avro schema definition (must match producer schema)
AVRO_SCHEMA = {
    "type": "record",
    "name": "HsuTestEvent",
    "fields": [
        {"name": "user", "type": "string"},
        {"name": "event", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "ts", "type": "long"},
    ],
}

spark = SparkSession.builder.appName("kafka-foreachBatch-avro").getOrCreate()

# ----- Paths -----
checkpoint = "./checkpoint_hsutest_avro"
output_path = "./data_avro"  # local directory for Parquet files

# Create output directory
os.makedirs(output_path, exist_ok=True)


def deserialize_avro_udf(avro_bytes):
    """
    UDF to deserialize Avro bytes back to a Row object.
    """
    if avro_bytes is None:
        return None

    try:
        bytes_reader = io.BytesIO(avro_bytes)
        record = fastavro.schemaless_reader(bytes_reader, AVRO_SCHEMA)
        return Row(
            user=record["user"],
            event=record["event"],
            amount=record["amount"],
            ts=record["ts"],
        )
    except Exception as e:
        # Return None for invalid records
        print(f"Error deserializing Avro record: {e}")
        return None


# Register UDF
deserialize_udf = udf(
    deserialize_avro_udf,
    StructType(
        [
            StructField("user", StringType(), True),
            StructField("event", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("ts", LongType(), True),
        ]
    ),
)

# 1) Kafka source
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "hsudemo")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100)  # ~100 records per micro-batch
    .load()
)

# 2) Parse Avro payload
parsed = (
    raw.select(
        deserialize_udf(col("value")).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
    )
    .select(
        col("data.user").alias("user"),
        col("data.event").alias("event"),
        col("data.amount").alias("amount"),
        col("data.ts").alias("ts"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_ts"),
    )
    .filter(col("user").isNotNull())
)  # Filter out failed deserializations


# 3) foreachBatch writer
def write_batch(batch_df, batch_id: int):
    print(f"Processing batch {batch_id} with {batch_df.count()} records")

    out = batch_df.withColumn("batch_id", lit(batch_id)).withColumn(
        "ingest_ts", current_timestamp()
    )

    (
        out.write.mode("append")
        .partitionBy("batch_id")  # one folder per batch
        .parquet(output_path)
    )

    print(f"Batch {batch_id} written to {output_path}")


# 4) Start query
print(f"Starting Avro streaming consumer...")
print(f"Reading from topic: hsudemo")
print(f"Output path: {output_path}")
print(f"Checkpoint: {checkpoint}")

query = (
    parsed.writeStream.foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start()
)

query.awaitTermination()
