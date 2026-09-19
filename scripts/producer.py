import csv
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock-quotes"
CSV_FILE_PATH = "data\indexProcessed.csv"


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def main():
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer
    )

    print(f"Starting producer... Reading records from {CSV_FILE_PATH}")

    with open(CSV_FILE_PATH, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Parse CSV row into target schema
            payload = {
                "ticker": row["Index"],
                "trade_timestamp": f"{row['Date']} 00:00:00",
                "price": float(row["Close"]),
                "volume": int(float(row["Volume"]))
            }

            # Send payload to Kafka
            producer.send(TOPIC_NAME, value=payload)
            print(f"Sent to Kafka: {payload}")
            
            # Simulate real-time streaming delay (1 second per record)
            time.sleep(1)


if __name__ == "__main__":
    main()