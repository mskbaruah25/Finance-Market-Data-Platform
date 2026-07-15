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

#loading environment variables
load_dotenv()


#Configure logging

logging.basicConfig(
    level = logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'

)

logger = logging.getLogger(__name__)

#Kafka variables

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
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


    def produce_to_kafka(self, symbol: str , period: str = "1y")-> Optional[pd.DataFrame]:
        try:
            self.logger.info(f"Fetching historical data for ticker symbol {symbol}")
            yf_ticker = yf.Ticker(symbol)

            #get historical data
            df = yf_ticker.history(period = period)

            df.reset_index(inplace = True)
            print(df)

        except Exception as e:
            self.logger.error(f"Failed to fetch historical data for symbol {symbol} {e}")
            return None

        pass



    def fetch_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        try:
            yf_ticker = yf.Ticker(symbol)
            df = yf_ticker.history(period=period)
            df.reset_index(inplace=True)
            return df
        except Exception as e:
            self.logger.error(f"Failed to fetch historical data for symbol {symbol}: {e}")
            return None




    def collectHistoricalData(self, period):
        stocks = tickers
        self.logger.info(f"Starting historical data collection for {len(tickers)} tickers")

        successfull_tickers = 0
        failed_tickers = 0

        for symbol in tickers:
            try:
                #fetch historical data
                df = self.fetch_historical_data(symbol, period)

                """if df is not None and not df.empty:
                    self.produce_to_kafka = (df, symbol)
                    successfull_tickers += 1
                else:
                    self.logger.warning(f"No data returned for symbol {symbol}")
                    failed_tickers += 1"""
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


