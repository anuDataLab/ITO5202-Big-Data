import argparse
import json
import time
from datetime import datetime, timezone

import pyarrow.parquet as pq
from kafka import KafkaProducer


def run_producer(data_path, batch_size, topic, broker):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda value: json.dumps(
            value, default=str
        ).encode("utf-8")
    )

    # Read the saved, held-out streaming dataset.
    dataset = pq.ParquetDataset(data_path)

    sent_total = 0

    for record_batch in dataset.read().to_batches(
        max_chunksize=batch_size
    ):
        records = record_batch.to_pylist()

        # One publication timestamp per batch.
        event_timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        # Publish the batch as a JSON array.
        for record in records:
            record["event_timestamp"] = event_timestamp

        producer.send(topic, value=records)
        producer.flush()

        sent_total += len(records)

        print(
            f"Time: {event_timestamp} | "
            f"Batch records: {len(records)} | "
            f"Total sent: {sent_total}",
            flush=True
        )

        time.sleep(5)

    producer.close()
    print("Streaming simulation completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data",
        default="data/stream_data.parquet"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200
    )
    parser.add_argument(
        "--topic",
        default="events"
    )
    parser.add_argument(
        "--broker",
        default="bigdata-kafka:9092"
    )

    args = parser.parse_args()

    run_producer(
        args.data,
        args.batch_size,
        args.topic,
        args.broker
    )