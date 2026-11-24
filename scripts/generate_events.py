import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime

BOOTSTRAP_SERVERS = "kafka-databricks-mkovaliou-de3b.d.aivencloud.com:11170"
TOPIC_NAME = "dev_raw_events"
CA_CERT_PATH = os.path.join(os.path.dirname(__file__), "../kafka_certs/ca.pem")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate events and send them to Kafka using confluent-kafka."
    )
    parser.add_argument(
        "--max-customer-id",
        type=int,
        default=5,
        help="Maximum customer ID for the range 1..max_customer_id (default: 5)",
    )
    parser.add_argument(
        "--topic-name",
        type=str,
        default=TOPIC_NAME,
        help="Topic name (default: dev_raw_events)",
    )
    return parser.parse_args()


def delivery_report(err, msg):
    if err is not None:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Message delivery failed: {err}",
            file=sys.stderr,
        )
    else:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Send event: delivered to {msg.topic()} "
            f"[partition {msg.partition()}] at offset {msg.offset()}"
        )


def main():
    args = parse_args()

    sasl_username = os.getenv("KAFKA_USER")
    if not sasl_username:
        print("Error: Environment variable KAFKA_USER is not set!", file=sys.stderr)
        sys.exit(1)

    sasl_password = os.getenv("KAFKA_PASSWORD")
    if not sasl_password:
        print("Error: Environment variable KAFKA_PASSWORD is not set!", file=sys.stderr)
        sys.exit(1)
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-256",
        "sasl.username": sasl_username,
        "sasl.password": sasl_password,
        "ssl.ca.location": CA_CERT_PATH,
    }

    print("Connecting to Aiven Kafka with confluent-kafka...")
    producer = Producer(conf)
    print(
        f"Producer successfully started. Begin generate events (max_customer_id={args.max_customer_id})..."
    )

    try:
        while True:
            currency = random.choices(
                ["USD", "BYN", "EUR"], weights=[0.475, 0.475, 0.05], k=1
            )[0]
            event = {
                "event_id": str(uuid.uuid4()),
                "customer_id": str(random.randint(1, args.max_customer_id)),
                "amount": round(random.uniform(10.0, 500.0), 2),
                "currency": currency,
                "date": datetime.now().strftime("%Y-%m-%d"),
            }

            producer.produce(
                args.topic_name,
                key=event["event_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            sleep_time = random.randint(10, 20)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nGeneration was stopped by user.")
    finally:
        print("Flushing remaining messages...")
        producer.flush()
        print("Producer closed.")


if __name__ == "__main__":
    main()
