import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


load_dotenv(Path(__file__).resolve().parents[3] / ".env")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH")
KAFKA_GROUP_BATCH_ID = os.getenv("KAFKA_GROUP_BATCH_ID")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = "minio:9000"


class BatchDataConsumer:
    def __init__(self,bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,topic=KAFKA_TOPIC_BATCH,group_id=KAFKA_GROUP_BATCH_ID):
        self.logger = logger
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.bucket_name = MINIO_BUCKET

    def create_minio_client(self):
        return Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

    def ensure_bucket_exists(self, minio_client):
        try:
            if not minio_client.bucket_exists(self.bucket_name):
                minio_client.make_bucket(self.bucket_name)
                self.logger.info(f"Created bucket {self.bucket_name}")
            else:
                self.logger.info(f"Bucket {self.bucket_name} already exists")
        except S3Error:
            self.logger.error("Error creating bucket")
            raise

    def create_kafka_consumer(self):
        if not self.bootstrap_servers or not self.topic or not self.group_id:
            raise RuntimeError(
                "Kafka settings missing. Check KAFKA_BOOTSTRAP_SERVERS, "
                "KAFKA_TOPIC_BATCH, and KAFKA_GROUP_BATCH_ID in .env"
            )

        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(config)
        consumer.subscribe([self.topic])
        return consumer

    def process_message(self, message, minio_client, consumer):
        data = json.loads(message.value().decode("utf-8"))
        symbol = data["symbol"]
        date = data["batch_date"]
        year, month, day = date.split("-")
        df = pd.DataFrame([data])

        object_name = (
            f"raw/historical/year={year}/month={month}/day={day}/"
            f"{symbol}_{datetime.now().strftime('%H%M%S')}.parquet"
        )
        parquet_file = f"/tmp/{symbol}.parquet"
        df.to_parquet(parquet_file, index=False)

        minio_client.fput_object(
            self.bucket_name,
            object_name,
            parquet_file,
        )
        self.logger.info(
            f"Wrote data for stock {symbol} to "
            f"s3://{self.bucket_name}/{object_name}"
        )
        os.remove(parquet_file)
        consumer.commit()

    def consume(self):
        minio_client = self.create_minio_client()
        self.ensure_bucket_exists(minio_client)
        consumer = self.create_kafka_consumer()

        try:
            while True:
                message = consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    self.logger.error(f"Consume error: {message.error()}")
                    continue
                if message.value() is None:
                    continue

                try:
                    self.process_message(message, minio_client, consumer)
                except Exception as error:
                    self.logger.error(f"Error processing message: {error}")
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer")
        finally:
            consumer.close()


def main():
    consumer = BatchDataConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC_BATCH,
        group_id=KAFKA_GROUP_BATCH_ID,
    )
    consumer.consume()


if __name__ == "__main__":
    main()
