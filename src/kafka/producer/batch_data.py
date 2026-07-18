import json
import os
import time
from datetime import datetime
import logging
import pandas as pd
import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv
from typing import Optional
from pathlib import Path

#loading environment variables
load_dotenv()


#Configure logging

logging.basicConfig(
    level = logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'

)

kite_api_key = os.environ["KITE_API_KEY"]
kite_api_secret = os.environ["KITE_API_SECRET"]
kite_login_url = os.environ["KITE_LOGIN_URL"]

logger = logging.getLogger(__name__)

#Kafka variables

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC_BATCH = os.getenv('KAFKA_TOPIC_BATCH')

#just sample stocks. We will have to develop a program to identify which stocks we want the data of
tickers = [
    # NSE stocks
    "RELIANCE.NS",
    "INFY.NS",
    "TCS.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "SBIN.NS",
    "BHARTIARTL.NS",
    "ITC.NS",
    "LT.NS",
    "MARUTI.NS",

    # NSE indices
    "^NSEI",    
    "^NSEBANK"     
]


class HistoricalDataCollector:
    def __init__(self, bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS, topic = KAFKA_TOPIC_BATCH):

        self.logger = logger
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers


        self.producer = {
            "bootstrap.servers" : bootstrap_servers,
            "client.id" : "historical_data_collector"
        }
        

        try:
            self.producer = Producer(self.producer)
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer {e}")
            raise


    def fetch_historical_data(self, symbol: str , period: str = "1y")-> Optional[pd.DataFrame]:
        try:
            self.logger.info(f"Fetching historical data for ticker symbol {symbol}")
            yf_ticker = yf.Ticker(symbol)

            #get historical data
            df = yf_ticker.history(period = period)

            df.reset_index(inplace = True)
            #maybe we change it in the production environment
            df.rename(
                columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                }, inplace=True
            )

            df['date'] = df['date'].dt.strftime("%Y-%m-%d")
            df['symbol'] = symbol
            df = df[['date', 'symbol' ,'open', 'high', 'low', 'close', 'volume' ]]

            self.logger.info(f"Successfully fetched {len(df)} days of data for stock {symbol}")

            return df

        except Exception as e:
            self.logger.error(f"Failed to fetch historical data for symbol {symbol} {e}")
            return None      



    def produce_to_kafka(self, df: pd.DataFrame, symbol: str):
        #we will use timestamp to create a batch id
        batch_id =  str(datetime.now().strftime("%Y%m%d%H%M%S"))
        df["batch_id"] = batch_id
        df["batch_date"] = datetime.now().strftime("%Y-%m-%d")
        records = df.to_dict(orient = "records")

        successfull_records = 0
        failed_records = 0

        for record in records:
            try:
                data = json.dumps(record)
                self.producer.produce(
                    topic = self.topic,
                    key = symbol,
                    value = data,
                    callback = self.delivery_report
                )

                self.producer.poll(1)

                successfull_records += 1

            except Exception as e:
                self.logger.error(f"Failed to produce message for stock symbol {symbol} {e}")


        self.producer.flush()
        self.logger.info(f"Successfully produced {successfull_records} records for stock {symbol} and failed {failed_records}")


    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message {msg}")
        else:
            self.logger.info(f"Message delivered successfully to topic {msg.topic} [{msg.partition()}]")



    def collectHistoricalData(self, period):
        stocks = tickers
        self.logger.info(f"Starting historical data collection for {len(tickers)} tickers")

        successfull_tickers = 0
        failed_tickers = 0

        for symbol in tickers:
            try:
                #fetch historical data
                df = self.fetch_historical_data(symbol, period)

                if df is not None and not df.empty:
                    self.produce_to_kafka(df, symbol)
                    successfull_tickers += 1
                else:
                    self.logger.warning(f"No data returned for symbol {symbol}")
                    failed_tickers += 1
            except Exception as e:
                self.logger.error(f"Error occured while processing the data of stock {symbol}: {e}")

            time.sleep(1)

        self.logger.info(f"Historical data collection completed. Successfull : {successfull_tickers}. Failed : {failed_tickers}")






def main():
    try:
        logger.info(f"Starting Historical Stock Data Collector")
        collector = HistoricalDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic = KAFKA_TOPIC_BATCH
        )
        collector.collectHistoricalData(period = "1y")
    except Exception as e:
        logger.error(f"Fatal error {e}")
        

if __name__ == "__main__":
     main()


