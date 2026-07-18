import json
import logging
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error

load_dotenv(Path(__file__).resolve().parents[3] / ".env")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC_BATCH = os.getenv('KAFKA_TOPIC_BATCH')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_BATCH_ID')  

MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')
MINIO_ENDPOINT = "localhost:9000"


def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure= False
    )

def ensure_bucket_exists(minio_client, bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
        else:
            logger.info(f"Bucket {bucket_name} already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket")
        raise


    

def main():
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)


    
    if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC_BATCH or not KAFKA_GROUP_ID:
        raise RuntimeError(
            "Kafka settings missing. Check KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_BATCH, and KAFKA_GROUP_BATCH_ID in .env"
        )

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consume error: {msg.error}")
                continue
            if msg.value() is None:
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                symbol = data['symbol']
                date = data['batch_date']
                year, month, day = date.split("-")
                df = pd.DataFrame([data])

                #savetominio
                object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}_{datetime.now().strftime('%H%M%S')}.parquet"
                parquet_file = f"/tmp/{symbol}.parquet"
                df.to_parquet(parquet_file, index=False)


                minio_client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    parquet_file
                )
                logger.info(f"Wrote data for stock {symbol} to s3://{MINIO_BUCKET}/{object_name}")
                os.remove(parquet_file)
                consumer.commit()

                
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
    












