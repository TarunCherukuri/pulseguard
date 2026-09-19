import argparse
import json
import time

from confluent_kafka import Consumer
from google.cloud import bigquery


PROJECT_ID = "project-28e97307-d56d-4dc8-995"
DATASET = "pulseguard"


def build_consumer(topic: str):
    return Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"pulseguard-bigquery-{topic}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })


def transform_transaction(event):
    return {
        "event_id": event.get("event_id"),
        "event_time": event.get("event_time"),
        "customer_id": event.get("customer_id"),
        "merchant_id": event.get("merchant_id"),
        "amount": event.get("amount"),
        "currency": event.get("currency"),
        "country": event.get("country"),
        "device_id": event.get("device_id"),
        "merchant_category": event.get("merchant_category"),
        "channel": event.get("channel"),
        "topic": event.get("topic"),
        "kafka_partition": event.get("partition"),
        "kafka_offset": event.get("offset"),
        "kafka_timestamp": event.get("kafka_timestamp"),
        "validated_at": event.get("validated_at"),
        "validation_batch_id": event.get("validation_batch_id"),
    }


def transform_anomaly(event):
    return {
        "customer_id": event.get("customer_id"),
        "window_start": event.get("window_start"),
        "window_end": event.get("window_end"),
        "txn_count_1m": event.get("txn_count_1m"),
        "txn_amount_sum_1m": event.get("txn_amount_sum_1m"),
        "txn_amount_avg_1m": event.get("txn_amount_avg_1m"),
        "txn_amount_max_1m": event.get("txn_amount_max_1m"),
        "txn_amount_stddev_1m": event.get("txn_amount_stddev_1m"),
        "unique_devices_1m": event.get("unique_devices_1m"),
        "unique_countries_1m": event.get("unique_countries_1m"),
        "amount_to_avg_ratio": event.get("amount_to_avg_ratio"),
        "velocity_flag": event.get("velocity_flag"),
        "amount_flag": event.get("amount_flag"),
        "anomaly_score": event.get("anomaly_score"),
        "anomaly_reason": event.get("anomaly_reason"),
        "detected_at": event.get("detected_at"),
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--topic",
        required=True,
        choices=[
            "validated-transaction-events",
            "anomaly-events",
        ],
    )

    args = parser.parse_args()

    table_name = (
        "transactions"
        if args.topic == "validated-transaction-events"
        else "anomalies"
    )

    transform = (
        transform_transaction
        if args.topic == "validated-transaction-events"
        else transform_anomaly
    )

    client = bigquery.Client(project=PROJECT_ID)
    consumer = build_consumer(args.topic)

    consumer.subscribe([args.topic])

    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    print("=" * 60)
    print("PulseGuard Kafka → BigQuery Sink")
    print("=" * 60)
    print(f"Kafka topic : {args.topic}")
    print(f"BigQuery    : {table_id}")
    print("Starting at : latest")
    print("=" * 60)

    try:
        while True:
            messages = consumer.consume(
                num_messages=100,
                timeout=2.0,
            )

            if not messages:
                continue

            rows = []
            offsets = []

            for message in messages:
                if message.error():
                    print(f"Kafka error: {message.error()}")
                    continue

                try:
                    event = json.loads(
                        message.value().decode("utf-8")
                    )

                    rows.append(transform(event))
                    offsets.append(message)

                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    print(f"Invalid Kafka message: {exc}")

            if not rows:
                continue

            errors = client.insert_rows_json(
                table_id,
                rows,
            )

            if errors:
                print(f"BigQuery insert errors: {errors}")
                time.sleep(2)
                continue

            # Commit each successfully loaded Kafka message.
            for message in offsets:
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            print(
                f"[LOADED] {len(rows)} rows -> "
                f"{table_name}"
            )

    except KeyboardInterrupt:
        print("\nStopping sink...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()