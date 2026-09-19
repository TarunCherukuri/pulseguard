import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = "transaction-events"

CHECKPOINT_LOCATION = (
    "/opt/pulseguard/checkpoints/"
    "kafka_validation_v3"
)

QUARANTINE_LOCATION = (
    "/opt/pulseguard/quarantine/"
    "invalid_events"
)


# ============================================================
# Transaction schema
# ============================================================

TRANSACTION_SCHEMA = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            True,
        ),
        StructField(
            "event_time",
            StringType(),
            True,
        ),
        StructField(
            "customer_id",
            StringType(),
            True,
        ),
        StructField(
            "merchant_id",
            StringType(),
            True,
        ),
        StructField(
            "amount",
            DoubleType(),
            True,
        ),
        StructField(
            "currency",
            StringType(),
            True,
        ),
        StructField(
            "country",
            StringType(),
            True,
        ),
        StructField(
            "device_id",
            StringType(),
            True,
        ),
        StructField(
            "merchant_category",
            StringType(),
            True,
        ),
        StructField(
            "channel",
            StringType(),
            True,
        ),
    ]
)


# ============================================================
# Spark
# ============================================================

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PulseGuardValidation")
        .master("local[*]")
        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .getOrCreate()
    )


# ============================================================
# Kafka source
# ============================================================

def create_kafka_stream(spark: SparkSession):
    return (
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


# ============================================================
# JSON parsing
# ============================================================

def parse_transactions(kafka_df):
    """
    Convert Kafka's binary fields into strings,
    then parse the JSON payload using our schema.
    """

    raw_events = kafka_df.select(
        F.col("key")
        .cast("string")
        .alias("kafka_key"),

        F.col("value")
        .cast("string")
        .alias("raw_value"),

        F.col("topic"),

        F.col("partition"),

        F.col("offset"),

        F.col("timestamp")
        .alias("kafka_timestamp"),
    )

    parsed_events = (
        raw_events
        .withColumn(
            "parsed",
            F.from_json(
                F.col("raw_value"),
                TRANSACTION_SCHEMA,
            ),
        )
        .withColumn(
            "json_parse_failed",
            (
                F.col("raw_value").isNotNull()
                & F.col("parsed").isNull()
            ),
        )
    )

    return parsed_events


# ============================================================
# Typed transaction DataFrame
# ============================================================

def create_typed_events(parsed_events):
    """
    Extract typed fields from the parsed JSON object.
    """

    typed_events = parsed_events.select(
        "kafka_key",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "raw_value",
        "json_parse_failed",

        F.col("parsed.event_id")
        .alias("event_id"),

        F.expr(
            "try_to_timestamp(parsed.event_time)"
        ).alias("event_time"),

        F.col("parsed.customer_id")
        .alias("customer_id"),

        F.col("parsed.merchant_id")
        .alias("merchant_id"),

        F.col("parsed.amount")
        .alias("amount"),

        F.col("parsed.currency")
        .alias("currency"),

        F.col("parsed.country")
        .alias("country"),

        F.col("parsed.device_id")
        .alias("device_id"),

        F.col("parsed.merchant_category")
        .alias("merchant_category"),

        F.col("parsed.channel")
        .alias("channel"),
    )

    return typed_events


# ============================================================
# Validation
# ============================================================

def is_missing_or_blank(column_name: str):
    """
    Return a Spark expression that checks whether
    a string column is null or contains only whitespace.
    """

    column = F.col(column_name)

    return (
        column.isNull()
        | (F.trim(column) == "")
    )


def validate_events(typed_events):
    """
    Add validation_reason and is_valid columns.

    A transaction is valid only if it satisfies every
    data-quality rule defined by this pipeline.
    """

    validated = (
        typed_events
        .withColumn(
            "validation_reason",
            F.concat_ws(
                "; ",

                F.when(
                    F.col("raw_value").isNull(),
                    F.lit("NULL_KAFKA_VALUE"),
                ),

                F.when(
                    F.col("json_parse_failed"),
                    F.lit("MALFORMED_JSON"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & is_missing_or_blank("event_id")
                    ),
                    F.lit("MISSING_EVENT_ID"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & F.col("event_time").isNull()
                    ),
                    F.lit("INVALID_EVENT_TIME"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & is_missing_or_blank("customer_id")
                    ),
                    F.lit("MISSING_CUSTOMER_ID"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & is_missing_or_blank("merchant_id")
                    ),
                    F.lit("MISSING_MERCHANT_ID"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & (
                            F.col("amount").isNull()
                            | F.isnan(F.col("amount"))
                            | (F.col("amount") < 0)
                        )
                    ),
                    F.lit("INVALID_AMOUNT"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & F.col("amount").isNotNull()
                        & F.col("currency").isNotNull()
                        & (
                            F.col("currency") != "INR"
                        )
                    ),
                    F.lit("UNSUPPORTED_CURRENCY"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & is_missing_or_blank("country")
                    ),
                    F.lit("MISSING_COUNTRY"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & is_missing_or_blank("device_id")
                    ),
                    F.lit("MISSING_DEVICE_ID"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & is_missing_or_blank(
                            "merchant_category"
                        )
                    ),
                    F.lit(
                        "MISSING_MERCHANT_CATEGORY"
                    ),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & F.col("channel").isNotNull()
                        & (
                            ~F.col("channel").isin(
                                "web",
                                "mobile",
                                "pos",
                            )
                        )
                    ),
                    F.lit("INVALID_CHANNEL"),
                ),

                F.when(
                    (
                        ~F.col("json_parse_failed")
                        & F.col("channel").isNull()
                    ),
                    F.lit("MISSING_CHANNEL"),
                ),
            ),
        )
        .withColumn(
            "is_valid",
            (
                F.col("raw_value").isNotNull()
                & ~F.col("json_parse_failed")

                & F.col("event_id").isNotNull()
                & (F.trim(F.col("event_id")) != "")

                & F.col("event_time").isNotNull()

                & F.col("customer_id").isNotNull()
                & (F.trim(F.col("customer_id")) != "")

                & F.col("merchant_id").isNotNull()
                & (F.trim(F.col("merchant_id")) != "")

                & F.col("amount").isNotNull()
                & ~F.isnan(F.col("amount"))
                & (F.col("amount") >= 0)

                & (F.col("currency") == "INR")

                & F.col("country").isNotNull()
                & (F.trim(F.col("country")) != "")

                & F.col("device_id").isNotNull()
                & (F.trim(F.col("device_id")) != "")

                & F.col("merchant_category").isNotNull()
                & (
                    F.trim(
                        F.col("merchant_category")
                    ) != ""
                )

                & F.col("channel").isin(
                    "web",
                    "mobile",
                    "pos",
                )
            ),
        )
    )

    return validated


# ============================================================
# Process one micro-batch
# ============================================================

def process_batch(
    batch_df,
    batch_id: int,
) -> None:

    # --------------------------------------------------------
    # Why persist?
    #
    # We're going to perform multiple actions on this batch:
    # count(), show(), and a write.
    #
    # Persisting prevents Spark from recomputing the batch
    # repeatedly.
    # --------------------------------------------------------

    batch_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    valid_df = batch_df.filter(
        F.col("is_valid")
    )

    invalid_df = batch_df.filter(
        ~F.col("is_valid")
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    # ---------------------------------------------------------------
    # Publish validated events to Kafka
    # ---------------------------------------------------------------

    validated_to_kafka = (
        valid_df
        .withColumn(
            "validated_at",
            F.current_timestamp()
        )
        .withColumn(
            "validation_batch_id",
            F.lit(batch_id).cast("long")
        )
        .select(
            "event_id",
            "event_time",
            "customer_id",
            "merchant_id",
            "amount",
            "currency",
            "country",
            "device_id",
            "merchant_category",
            "channel",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "validated_at",
            "validation_batch_id",
        )
        .select(
            F.col("event_id").alias("key"),
            F.to_json(
                F.struct(
                    F.col("event_id"),
                    F.col("event_time"),
                    F.col("customer_id"),
                    F.col("merchant_id"),
                    F.col("amount"),
                    F.col("currency"),
                    F.col("country"),
                    F.col("device_id"),
                    F.col("merchant_category"),
                    F.col("channel"),
                    F.col("topic"),
                    F.col("partition"),
                    F.col("offset"),
                    F.col("kafka_timestamp"),
                    F.col("validated_at"),
                    F.col("validation_batch_id"),
                )
            ).alias("value"),
        )
    )

    validated_to_kafka.write \
        .format("kafka") \
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        ) \
        .option(
            "topic",
            "validated-transaction-events"
        ) \
        .save()

    print(
        f"Validated events published: {valid_count}"
    )

    print()
    print("=" * 70)
    print(
        f"VALIDATION BATCH {batch_id}"
    )
    print("=" * 70)
    print(
        f"Valid events:   {valid_count}"
    )
    print(
        f"Invalid events: {invalid_count}"
    )

    # --------------------------------------------------------
    # Display valid events
    # --------------------------------------------------------

    if valid_count > 0:

        print()
        print("VALID EVENTS")
        print("-" * 70)

        valid_df.select(
            "event_id",
            "event_time",
            "customer_id",
            "amount",
            "currency",
            "country",
            "device_id",
            "merchant_category",
            "channel",
        ).show(
            10,
            truncate=False,
        )

    # --------------------------------------------------------
    # Quarantine invalid events
    # --------------------------------------------------------

    if invalid_count > 0:

        print()
        print("INVALID EVENTS → QUARANTINE")
        print("-" * 70)

        invalid_output = (
            invalid_df
            .withColumn(
                "quarantine_batch_id",
                F.lit(batch_id),
            )
            .withColumn(
                "quarantined_at",
                F.current_timestamp(),
            )
            .select(
                "quarantine_batch_id",
                "quarantined_at",
                "validation_reason",
                "event_id",
                "customer_id",
                "raw_value",
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
            )
        )

        invalid_output.show(
            20,
            truncate=False,
        )

        (
            invalid_output.write
            .mode("append")
            .json(QUARANTINE_LOCATION)
        )

    batch_df.unpersist()


# ============================================================
# Main
# ============================================================

def main() -> None:

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    # --------------------------------------------------------
    # Kafka
    # --------------------------------------------------------

    kafka_df = create_kafka_stream(
        spark
    )

    # --------------------------------------------------------
    # Parse
    # --------------------------------------------------------

    parsed_events = parse_transactions(
        kafka_df
    )

    # --------------------------------------------------------
    # Type
    # --------------------------------------------------------

    typed_events = create_typed_events(
        parsed_events
    )

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    validated_events = validate_events(
        typed_events
    )

    # --------------------------------------------------------
    # Print schema once
    # --------------------------------------------------------

    validated_events.printSchema()

    # --------------------------------------------------------
    # Streaming query
    # --------------------------------------------------------

    query = (
        validated_events.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION,
        )
        .trigger(
            processingTime="5 seconds"
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()