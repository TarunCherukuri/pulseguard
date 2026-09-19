import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
)


# ================================================================
# Configuration
# ================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = "validated-transaction-events"

CHECKPOINT_LOCATION = (
    "/opt/pulseguard/checkpoints/features_v1"
)


# ================================================================
# Spark
# ================================================================

spark = (
    SparkSession.builder
    .appName("PulseGuardFeatureEngineering")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ================================================================
# Schema
# ================================================================

VALIDATED_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("channel", StringType(), True),

    # Kafka lineage
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", StringType(), True),

    # Validation lineage
    StructField("validated_at", StringType(), True),
    StructField("validation_batch_id", LongType(), True),
])


# ================================================================
# Read validated events from Kafka
# ================================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option(
        "subscribe",
        KAFKA_TOPIC,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .option(
        "failOnDataLoss",
        "true",
    )
    .load()
)


# ================================================================
# Kafka -> JSON
# ================================================================

parsed_stream = (
    raw_stream
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_value",
        "topic AS source_topic",
        "partition AS source_partition",
        "offset AS source_offset",
        "timestamp AS kafka_received_at",
    )
    .withColumn(
        "parsed",
        F.from_json(
            F.col("raw_value"),
            VALIDATED_SCHEMA,
        ),
    )
)


# ================================================================
# Extract typed event fields
# ================================================================

events = (
    parsed_stream
    .select(
        "kafka_key",
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_received_at",
        "parsed.*",
    )
    .withColumn(
        "event_time",
        F.expr(
            "try_to_timestamp(event_time)"
        ),
    )
    .filter(
        F.col("event_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("amount").isNotNull()
    )
)


# ================================================================
# Stateful customer-level windows
# ================================================================

features = (
    events
    .withWatermark(
        "event_time",
        "2 minutes",
    )
    .groupBy(
        F.window(
            "event_time",
            "1 minute",
            "10 seconds",
        ),
        "customer_id",
    )
    .agg(
        F.count("*").alias(
            "txn_count_1m"
        ),

        F.sum("amount").alias(
            "txn_amount_sum_1m"
        ),

        F.avg("amount").alias(
            "txn_amount_avg_1m"
        ),

        F.max("amount").alias(
            "txn_amount_max_1m"
        ),

        F.stddev_pop("amount").alias(
            "txn_amount_stddev_1m"
        ),

        F.approx_count_distinct(
            "device_id",
            0.05
        ).alias(
            "unique_devices_1m"
        ),

        F.approx_count_distinct(
            "country",
            0.05
        ).alias(
            "unique_countries_1m"
        ),
    )
)


# ================================================================
# Derived anomaly features
# ================================================================

features = (
    features

    # Largest transaction relative to average
    .withColumn(
        "amount_to_avg_ratio",
        F.when(
            F.col("txn_amount_avg_1m") > 0,
            (
                F.col("txn_amount_max_1m")
                /
                F.col("txn_amount_avg_1m")
            ),
        ).otherwise(0.0),
    )

    # Initial velocity rule
    .withColumn(
        "velocity_flag",
        F.col("txn_count_1m") >= 6,
    )

    # Initial amount rule
    .withColumn(
        "amount_flag",
        (
            F.col("txn_amount_max_1m") >= 100000
        )
        |
        (
            (
                F.col("txn_amount_avg_1m") > 0
            )
            &
            (
                F.col("amount_to_avg_ratio") >= 5
            )
        ),
    )

    # Number of triggered signals
    .withColumn(
        "anomaly_score",
        (
            F.col("velocity_flag").cast("int")
            +
            F.col("amount_flag").cast("int")
        ),
    )

    # Human-readable reason
    .withColumn(
        "anomaly_reason",
        F.concat_ws(
            ", ",
            F.when(
                F.col("velocity_flag"),
                F.lit("VELOCITY_SPIKE"),
            ),
            F.when(
                F.col("amount_flag"),
                F.lit("AMOUNT_SPIKE"),
            ),
        ),
    )
)


# ================================================================
# Console output
# ================================================================

query = (
    features
    .select(
        "window",
        "customer_id",
        "txn_count_1m",
        "txn_amount_sum_1m",
        "txn_amount_avg_1m",
        "txn_amount_max_1m",
        "txn_amount_stddev_1m",
        "unique_devices_1m",
        "unique_countries_1m",
        "amount_to_avg_ratio",
        "velocity_flag",
        "amount_flag",
        "anomaly_score",
        "anomaly_reason",
    )
    .writeStream
    .format("console")
    .outputMode("update")
    .option(
        "truncate",
        "false",
    )
    .option(
        "numRows",
        50,
    )
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)


query.awaitTermination()