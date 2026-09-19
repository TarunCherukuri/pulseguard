from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)

# ============================================================
# PulseGuard - Anomaly Event Stream
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "validated-transaction-events"
ANOMALY_TOPIC = "anomaly-events"

CHECKPOINT_LOCATION = "/opt/pulseguard/checkpoints/anomaly_v1"


# ------------------------------------------------------------
# Spark session
# ------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("PulseGuardAnomalyDetection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------
# Schema of validated Kafka events
# ------------------------------------------------------------

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
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", StringType(), True),
    StructField("validated_at", StringType(), True),
    StructField("validation_batch_id", LongType(), True),
])


# ------------------------------------------------------------
# Read validated events from Kafka
# ------------------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "true")
    .load()
)


# ------------------------------------------------------------
# Parse JSON payload
# ------------------------------------------------------------

events = (
    raw_stream
    .select(
        F.col("value").cast("string").alias("json_value")
    )
    .select(
        F.from_json(
            F.col("json_value"),
            VALIDATED_SCHEMA
        ).alias("event")
    )
    .select("event.*")
    .withColumn(
        "event_time",
        F.expr("try_to_timestamp(event_time)")
    )
    .filter(
        F.col("event_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("amount").isNotNull()
    )
)


# ------------------------------------------------------------
# Stateful feature engineering
#
# 1-minute window
# sliding every 10 seconds
# watermark = 2 minutes
# ------------------------------------------------------------

windowed_features = (
    events
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        F.window(
            F.col("event_time"),
            "1 minute",
            "10 seconds"
        ),
        F.col("customer_id")
    )
    .agg(
        F.count("*").alias("txn_count_1m"),
        F.sum("amount").alias("txn_amount_sum_1m"),
        F.avg("amount").alias("txn_amount_avg_1m"),
        F.max("amount").alias("txn_amount_max_1m"),
        F.stddev_pop("amount").alias("txn_amount_stddev_1m"),

        F.approx_count_distinct(
            "device_id",
            0.05
        ).alias("unique_devices_1m"),

        F.approx_count_distinct(
            "country",
            0.05
        ).alias("unique_countries_1m"),
    )
)


# ------------------------------------------------------------
# Derived anomaly signals
# ------------------------------------------------------------

features = (
    windowed_features
    .withColumn(
        "amount_to_avg_ratio",
        F.when(
            F.col("txn_amount_avg_1m") > 0,
            F.col("txn_amount_max_1m")
            / F.col("txn_amount_avg_1m")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "velocity_flag",
        F.col("txn_count_1m") >= 6
    )
    .withColumn(
        "amount_flag",
        (
            (F.col("txn_amount_max_1m") >= 100000)
            |
            (F.col("amount_to_avg_ratio") >= 5)
        )
    )
    .withColumn(
        "anomaly_score",
        F.col("velocity_flag").cast("int")
        + F.col("amount_flag").cast("int")
    )
    .withColumn(
        "anomaly_reason",
        F.when(
            F.col("velocity_flag")
            & F.col("amount_flag"),
            F.lit("VELOCITY_AND_AMOUNT_SPIKE")
        )
        .when(
            F.col("velocity_flag"),
            F.lit("VELOCITY_SPIKE")
        )
        .when(
            F.col("amount_flag"),
            F.lit("AMOUNT_SPIKE")
        )
    )
)


# ------------------------------------------------------------
# Keep only anomaly windows
# ------------------------------------------------------------

anomalies = (
    features
    .filter(F.col("anomaly_score") > 0)
)


# ------------------------------------------------------------
# Convert anomaly records to Kafka JSON
# ------------------------------------------------------------

anomaly_json = (
    anomalies
    .select(
        F.to_json(
            F.struct(
                F.col("customer_id"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),

                F.col("txn_count_1m"),
                F.col("txn_amount_sum_1m"),
                F.col("txn_amount_avg_1m"),
                F.col("txn_amount_max_1m"),
                F.col("txn_amount_stddev_1m"),

                F.col("unique_devices_1m"),
                F.col("unique_countries_1m"),
                F.col("amount_to_avg_ratio"),

                F.col("velocity_flag"),
                F.col("amount_flag"),
                F.col("anomaly_score"),
                F.col("anomaly_reason"),

                F.current_timestamp().alias("detected_at")
            )
        ).alias("value")
    )
)


# ------------------------------------------------------------
# Write anomaly events to Kafka
# ------------------------------------------------------------

query = (
    anomaly_json
    .writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("topic", ANOMALY_TOPIC)
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION
    )
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)


print("============================================================")
print("PulseGuard Anomaly Detection Stream")
print("============================================================")
print(f"Input topic : {KAFKA_TOPIC}")
print(f"Output topic: {ANOMALY_TOPIC}")
print("Window      : 1 minute")
print("Slide       : 10 seconds")
print("Watermark   : 2 minutes")
print("============================================================")

query.awaitTermination()