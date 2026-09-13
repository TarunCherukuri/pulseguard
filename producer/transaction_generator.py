import argparse
import json
import random
import signal
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transaction-events"

DEFAULT_EVENTS_PER_SECOND = 5
DEFAULT_ANOMALY_RATE = 0.10

GROUND_TRUTH_DIR = Path(__file__).parent / "output"
GROUND_TRUTH_FILE = GROUND_TRUTH_DIR / "ground_truth.jsonl"

CUSTOMER_DATA_DIR = Path(__file__).parent / "data"
CUSTOMER_PROFILES_FILE = CUSTOMER_DATA_DIR / "customers.json"


# ============================================================
# CUSTOMER PROFILE
# ============================================================

@dataclass
class CustomerProfile:
    customer_id: str
    average_amount: float
    amount_stddev: float
    country: str
    device_ids: list[str]
    preferred_categories: list[str]


# ============================================================
# STATIC DATA
# ============================================================

COUNTRIES = [
    "IN",
    "US",
    "GB",
    "SG",
    "AE",
]

MERCHANT_CATEGORIES = [
    "grocery",
    "food",
    "transport",
    "electronics",
    "travel",
    "jewelry",
    "health",
]

CHANNELS = [
    "mobile",
    "web",
    "pos",
]


# ============================================================
# GLOBAL SHUTDOWN FLAG
# ============================================================

running = True


def handle_shutdown(signum, frame):
    """
    Allows Ctrl+C to stop the generator cleanly.
    """
    global running
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ============================================================
# CUSTOMER CREATION
# ============================================================

# ============================================================
# CUSTOMER CREATION
# ============================================================

def create_customer_profiles(
    number_of_customers: int,
) -> list[CustomerProfile]:
    """
    Create synthetic customer profiles.

    Each customer gets their own:
    - spending baseline
    - country
    - devices
    - preferred merchant categories
    """

    profiles = []

    for i in range(1, number_of_customers + 1):

        average_amount = random.uniform(
            500,
            15000,
        )

        amount_stddev = (
            average_amount
            * random.uniform(0.15, 0.40)
        )

        country = random.choice(
            ["IN", "IN", "IN", "US", "GB"]
        )

        device_ids = [
            f"device_{i:04d}_01"
        ]

        if random.random() < 0.25:

            device_ids.append(
                f"device_{i:04d}_02"
            )

        preferred_categories = random.sample(
            MERCHANT_CATEGORIES,
            k=random.randint(2, 4),
        )

        profiles.append(
            CustomerProfile(
                customer_id=f"cust_{i:04d}",
                average_amount=average_amount,
                amount_stddev=amount_stddev,
                country=country,
                device_ids=device_ids,
                preferred_categories=preferred_categories,
            )
        )

    return profiles


def save_customer_profiles(
    profiles: list[CustomerProfile],
) -> None:
    """
    Save customer profiles to disk as JSON.

    This makes customer behavior persistent across
    generator restarts.
    """

    CUSTOMER_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    serialized_profiles = [
        asdict(profile)
        for profile in profiles
    ]

    with CUSTOMER_PROFILES_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            serialized_profiles,
            file,
            indent=2,
        )


def load_customer_profiles() -> list[CustomerProfile]:
    """
    Load previously generated customer profiles
    from disk.
    """

    with CUSTOMER_PROFILES_FILE.open(
        "r",
        encoding="utf-8",
    ) as file:

        raw_profiles = json.load(file)

    return [
        CustomerProfile(
            customer_id=profile["customer_id"],
            average_amount=profile["average_amount"],
            amount_stddev=profile["amount_stddev"],
            country=profile["country"],
            device_ids=profile["device_ids"],
            preferred_categories=profile[
                "preferred_categories"
            ],
        )
        for profile in raw_profiles
    ]


def get_customer_profiles(
    number_of_customers: int = 100,
) -> list[CustomerProfile]:
    """
    Load existing customer profiles.

    If they don't exist yet, create them and persist them.
    """

    if CUSTOMER_PROFILES_FILE.exists():

        print(
            f"Loading customer profiles from "
            f"{CUSTOMER_PROFILES_FILE}"
        )

        profiles = load_customer_profiles()

        if len(profiles) != number_of_customers:

            raise ValueError(
                "Customer profile count does not match "
                f"expected value of {number_of_customers}."
            )

        return profiles

    print(
        "No customer profile file found. "
        "Generating new customer profiles..."
    )

    profiles = create_customer_profiles(
        number_of_customers
    )

    save_customer_profiles(profiles)

    print(
        f"Saved customer profiles to "
        f"{CUSTOMER_PROFILES_FILE}"
    )

    return profiles


# ============================================================
# TRANSACTION GENERATION
# ============================================================

def generate_normal_transaction(
    customer: CustomerProfile,
) -> dict:
    """
    Generate a transaction that should look normal
    for the selected customer.
    """

    amount = random.gauss(
        customer.average_amount,
        customer.amount_stddev,
    )

    # A payment amount should never be negative.
    amount = max(10.0, amount)

    merchant_category = random.choice(
        customer.preferred_categories
    )

    transaction = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer.customer_id,
        "merchant_id": f"merchant_{random.randint(1, 200):04d}",
        "amount": round(amount, 2),
        "currency": "INR",
        "country": customer.country,
        "device_id": random.choice(customer.device_ids),
        "merchant_category": merchant_category,
        "channel": random.choice(CHANNELS),
    }

    return transaction


# ============================================================
# ANOMALY GENERATORS
# ============================================================

def inject_amount_spike(
    transaction: dict,
    customer: CustomerProfile,
) -> str:
    """
    Make the transaction amount dramatically larger
    than the customer's normal behavior.
    """

    transaction["amount"] = round(
        customer.average_amount
        * random.uniform(8, 20),
        2,
    )

    return "AMOUNT_SPIKE"


def inject_unusual_country(
    transaction: dict,
    customer: CustomerProfile,
) -> str:
    """
    Move the transaction to a country that differs
    from the customer's normal country.
    """

    unusual_countries = [
        country
        for country in COUNTRIES
        if country != customer.country
    ]

    transaction["country"] = random.choice(unusual_countries)

    return "UNUSUAL_COUNTRY"


def inject_new_device(
    transaction: dict,
) -> str:
    """
    Introduce a previously unseen device ID.
    """

    transaction["device_id"] = (
        f"unknown_device_{uuid.uuid4().hex[:10]}"
    )

    return "NEW_DEVICE"


def inject_combined_anomaly(
    transaction: dict,
    customer: CustomerProfile,
) -> str:
    """
    Combine several suspicious signals into one event.
    """

    transaction["amount"] = round(
        customer.average_amount
        * random.uniform(10, 25),
        2,
    )

    unusual_countries = [
        country
        for country in COUNTRIES
        if country != customer.country
    ]

    transaction["country"] = random.choice(
        unusual_countries
    )

    transaction["device_id"] = (
        f"unknown_device_{uuid.uuid4().hex[:10]}"
    )

    transaction["merchant_category"] = "jewelry"

    return "COMBINED_ANOMALY"


# ============================================================
# PRODUCER CALLBACK
# ============================================================

def delivery_report(
    err,
    msg,
    event_id: str,
    customer_id: str,
    anomaly_type: str,
):
    """
    Called by Kafka after attempting to deliver a message.

    Ground truth is recorded only after successful delivery.
    """

    if err is not None:
        print(
            f"[KAFKA ERROR] "
            f"event_id={event_id} "
            f"customer_id={customer_id} "
            f"error={err}"
        )
        return

    print(
        f"[DELIVERED] "
        f"event_id={event_id} "
        f"customer_id={customer_id} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()} "
        f"ground_truth={anomaly_type}"
    )

    write_ground_truth(
        event_id=event_id,
        anomaly_type=anomaly_type,
    )

# ============================================================
# GROUND TRUTH
# ============================================================

def write_ground_truth(
    event_id: str,
    anomaly_type: str,
):
    """
    Save ground truth outside the Kafka event itself.

    This allows us to evaluate the detector later without
    giving the detector the answer.
    """

    GROUND_TRUTH_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    record = {
        "event_id": event_id,
        "ground_truth_anomaly": anomaly_type,
        "recorded_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    with GROUND_TRUTH_FILE.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(record) + "\n"
        )


# ============================================================
# PUBLISH ONE EVENT
# ============================================================

def publish_transaction(
    producer: Producer,
    customer: CustomerProfile,
    anomaly_type: str | None = None,
):
    """
    Generate one transaction and publish it to Kafka.
    """

    transaction = generate_normal_transaction(
        customer
    )

    if anomaly_type == "AMOUNT_SPIKE":
        ground_truth = inject_amount_spike(
            transaction,
            customer,
        )

    elif anomaly_type == "UNUSUAL_COUNTRY":
        ground_truth = inject_unusual_country(
            transaction,
            customer,
        )

    elif anomaly_type == "NEW_DEVICE":
        ground_truth = inject_new_device(
            transaction,
        )

    elif anomaly_type == "COMBINED_ANOMALY":
        ground_truth = inject_combined_anomaly(
            transaction,
            customer,
        )

    else:
        ground_truth = "NONE"

    payload = json.dumps(
        transaction
    ).encode("utf-8")

    event_id = transaction["event_id"]
    customer_id = customer.customer_id

    producer.produce(
        topic=KAFKA_TOPIC,
        key=customer_id,
        value=payload,
        callback=lambda err, msg: delivery_report(
            err,
            msg,
            event_id,
            customer_id,
            ground_truth,
        ),
    )

    producer.poll(0)

# ============================================================
# VELOCITY ANOMALY
# ============================================================

def publish_velocity_burst(
    producer: Producer,
    customer: CustomerProfile,
    burst_size: int = 10,
):
    """
    Generate a burst of transactions for one customer.

    The transactions themselves are intentionally kept
    relatively normal. The anomaly comes from their frequency.
    """

    print(
        f"[VELOCITY BURST] "
        f"customer_id={customer.customer_id} "
        f"events={burst_size}"
    )

    for _ in range(burst_size):

        transaction = generate_normal_transaction(
            customer
        )

        payload = json.dumps(
            transaction
        ).encode("utf-8")

        event_id = transaction["event_id"]
        customer_id = customer.customer_id

        anomaly_type = "VELOCITY_SPIKE"

        producer.produce(
            topic=KAFKA_TOPIC,
            key=customer_id,
            value=payload,
            callback=lambda err, msg: delivery_report(
                err,
                msg,
                event_id,
                customer_id,
                anomaly_type,
            ),
        )

        producer.poll(0)

# ============================================================
# GENERATOR LOOP
# ============================================================

def run_generator(
    events_per_second: float,
    anomaly_rate: float,
):
    """
    Continuously generate transactions and send them
    to Kafka.
    """

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "pulseguard-transaction-generator",
        }
    )

    customers = get_customer_profiles(
        number_of_customers=100
    )

    delay = 1 / events_per_second

    print("=" * 60)
    print("PulseGuard Transaction Generator")
    print("=" * 60)

    print(
        f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic: {KAFKA_TOPIC}"
    )

    print(
        f"Events/sec: {events_per_second}"
    )

    print(
        f"Anomaly rate: {anomaly_rate:.0%}"
    )

    print(
        f"Ground truth: {GROUND_TRUTH_FILE}"
    )

    print("=" * 60)
    print("Press Ctrl+C to stop.")
    print()

    while running:

        customer = random.choice(customers)

        anomaly_type = None

        if random.random() < anomaly_rate:

            anomaly_type = random.choice(
                [
                    "AMOUNT_SPIKE",
                    "UNUSUAL_COUNTRY",
                    "NEW_DEVICE",
                    "VELOCITY_SPIKE",
                    "COMBINED_ANOMALY",
                ]
            )

            if anomaly_type == "VELOCITY_SPIKE":

                burst_size = random.randint(
                    8,
                    12,
                )

                publish_velocity_burst(
                    producer=producer,
                    customer=customer,
                    burst_size=burst_size,
                )

            else:

                publish_transaction(
                    producer=producer,
                    customer=customer,
                    anomaly_type=anomaly_type,
                )

        else:

            publish_transaction(
                producer=producer,
                customer=customer,
                anomaly_type=None,
            )

        time.sleep(delay)

    print()
    print("Stopping generator...")

    producer.flush(
        timeout=10
    )

    print("Generator stopped cleanly.")


# ============================================================
# COMMAND-LINE INTERFACE
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic payment transactions "
            "for the PulseGuard streaming pipeline."
        )
    )

    parser.add_argument(
        "--events-per-second",
        type=float,
        default=DEFAULT_EVENTS_PER_SECOND,
    )

    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=DEFAULT_ANOMALY_RATE,
    )

    return parser.parse_args()


def main():

    args = parse_arguments()

    if args.events_per_second <= 0:
        raise ValueError(
            "events-per-second must be greater than 0."
        )

    if not 0 <= args.anomaly_rate <= 1:
        raise ValueError(
            "anomaly-rate must be between 0 and 1."
        )

    run_generator(
        events_per_second=args.events_per_second,
        anomaly_rate=args.anomaly_rate,
    )


if __name__ == "__main__":
    main()