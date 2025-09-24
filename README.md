# Lab06 - Avro Streaming

This lab converts the original JSON-based Kafka producer and Spark consumer to use Avro serialization format.

## Files

-   `send_hsutest_events_avro.py` - Async Kafka producer using Avro serialization
-   `spark_kafka_foreachbatch_avro.py` - Spark Streaming consumer with Avro deserialization
-   `requirements.txt` - Python dependencies

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Make sure Kafka is running on `localhost:9092`

## Usage

### Producer

Run the Avro producer:

```bash
python send_hsutest_events_avro.py --bootstrap localhost:9092 --topic hsudemo --rps 10
```

Options:

-   `--bootstrap`: Kafka bootstrap servers (default: localhost:9092)
-   `--topic`: Kafka topic name (default: hsudemo)
-   `--rps`: Messages per second (default: 20)
-   `--seed`: Random seed for reproducibility
-   `--key-by-user`: Use user field as Kafka message key

### Consumer

Run the Spark consumer:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_kafka_foreachbatch_avro.py
```

## Schema

The Avro schema used for messages:

```json
{
    "type": "record",
    "name": "HsuTestEvent",
    "fields": [
        { "name": "user", "type": "string" },
        { "name": "event", "type": "string" },
        { "name": "amount", "type": "double" },
        { "name": "ts", "type": "long" }
    ]
}
```

## Changes from JSON Version

### Producer Changes:

1. Added `fastavro` import and Avro schema definition
2. Created `serialize_avro()` function using `fastavro.schemaless_writer`
3. Updated producer to use Avro serializer instead of JSON

### Consumer Changes:

1. Added `fastavro` import and matching Avro schema
2. Created `deserialize_avro_udf()` UDF for Avro deserialization using `fastavro.schemaless_reader`
3. Updated data parsing to use Avro UDF instead of JSON parsing
4. Added error handling for failed deserializations
5. Changed output directory to `data_avro` and checkpoint to `checkpoint_hsutest_avro`

## Output

-   Producer: Sends Avro-serialized messages to Kafka topic
-   Consumer: Reads Avro messages and writes to Parquet files in `./data_avro/` directory
-   Data is partitioned by batch_id with additional metadata (ingest_ts, kafka_ts, etc.)
