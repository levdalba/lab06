# spark_kafka_foreachbatch_data.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, current_timestamp, lit

# Ensure local "data" directory exists
os.makedirs("data", exist_ok=True)

spark = SparkSession.builder.appName("kafka-foreachBatch-data").getOrCreate()

# ----- Paths -----
checkpoint = "./checkpoint_hsutest"
output_path = "./data"  # local directory for Parquet files

# 1) Kafka source
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "hsudemo")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100)  # ~100 records per micro-batch
    .load()
)

# 2) Parse JSON payload
schema = StructType(
    [
        StructField("user", StringType(), True),
        StructField("event", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("ts", LongType(), True),
    ]
)

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_ts"),
).select(col("data.*"), col("topic"), col("partition"), col("offset"), col("kafka_ts"))


# 3) foreachBatch writer
def write_batch(batch_df, batch_id: int):
    out = batch_df.withColumn("batch_id", lit(batch_id)).withColumn(
        "ingest_ts", current_timestamp()
    )

    (
        out.write.mode("append")
        .partitionBy("batch_id")  # one folder per batch
        .parquet(output_path)
    )


# 4) Start query
query = (
    parsed.writeStream.foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start()
)

query.awaitTermination()
