import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker
from kiteconnect.exceptions import TokenException


load_dotenv(Path(__file__).resolve().parents[3] / ".env")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME")
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

TICKERS = [
    "RELIANCE",
    "INFY",
    "TCS",
    "HDFCBANK",
    "ICICIBANK",
    "SBIN",
    "BHARTIARTL",
    "ITC",
    "LT",
    "MARUTI",
    "NIFTY 50",
    "NIFTY BANK",
]


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


class StreamDataCollector:
    def __init__(
        self,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC_REALTIME,
        api_key=KITE_API_KEY,
        access_token=KITE_ACCESS_TOKEN,
    ):
        self.logger = logger
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.access_token = access_token
        self.tickers = TICKERS.copy()

    def authenticate_kite(self):
        if not self.api_key or not self.access_token:
            self.logger.error("Kite API key or access token is missing")
            raise RuntimeError(
                "Kite credentials are missing. Run "
                "'python src/utilities/generate_access_token.py' and add the "
                "new KITE_ACCESS_TOKEN to .env."
            )

        self.logger.info("Validating Kite access token")
        try:
            kite = KiteConnect(api_key=self.api_key)
            kite.set_access_token(self.access_token)
            profile = kite.profile()
        except TokenException as error:
            self.logger.error("Kite access token is invalid or expired")
            raise RuntimeError(
                "KITE_ACCESS_TOKEN is invalid or expired. Run "
                "'python src/utilities/generate_access_token.py' again."
            ) from error
        except Exception:
            self.logger.exception("Failed to validate Kite credentials")
            raise

        self.logger.info(
            f"Kite authentication successful for user {profile['user_id']}"
        )
        return kite

    def create_kafka_producer(self):
        if not self.bootstrap_servers or not self.topic:
            self.logger.error("Kafka bootstrap server or real-time topic is missing")
            raise RuntimeError(
                "Kafka settings missing. Check KAFKA_BOOTSTRAP_SERVERS and "
                "KAFKA_TOPIC_REALTIME in .env."
            )

        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "stream_data_producer",
        }
        try:
            producer = Producer(producer_config)
        except Exception:
            self.logger.exception("Failed to create Kafka producer")
            raise

        self.logger.info(
            f"Kafka producer initialized. Sending to: {self.bootstrap_servers}"
        )
        return producer

    def get_instrument_tokens(self, kite):
        self.logger.info("Loading NSE instruments from Kite")
        try:
            instruments = kite.instruments("NSE")
        except Exception:
            self.logger.exception("Failed to load NSE instruments from Kite")
            raise

        token_to_symbol = {}
        wanted_symbols = set(self.tickers)
        found_symbols = set()
        for instrument in instruments:
            symbol = instrument["tradingsymbol"]
            if symbol in wanted_symbols:
                token = int(instrument["instrument_token"])
                token_to_symbol[token] = f"NSE:{symbol}"
                found_symbols.add(symbol)

        missing_symbols = [
            symbol for symbol in self.tickers if symbol not in found_symbols
        ]

        if missing_symbols:
            self.logger.warning(
                f"Kite instruments not found: {', '.join(missing_symbols)}"
            )
        if not token_to_symbol:
            self.logger.error("No configured ticker was found in Kite")
            raise RuntimeError("None of the configured tickers were found in Kite.")

        self.logger.info(f"Found {len(token_to_symbol)} Kite instruments")
        return token_to_symbol

    def delivery_report(self, error, message):
        if error is not None:
            self.logger.error(f"Delivery failed for message: {error}")
        else:
            self.logger.debug(
                f"Message delivered to {message.topic()} [{message.partition()}]"
            )

    def on_connect(self, websocket, response):
        instrument_tokens = list(self.token_to_symbol)
        try:
            websocket.subscribe(instrument_tokens)
            websocket.set_mode(websocket.MODE_QUOTE, instrument_tokens)
        except Exception:
            self.logger.exception("Failed to subscribe to Kite instruments")
            raise

        self.logger.info(
            f"Subscribed to {len(instrument_tokens)} Kite instruments"
        )

    def on_ticks(self, websocket, ticks):
        self.logger.debug(f"Received {len(ticks)} tick(s) from Kite")
        for tick in ticks:
            symbol = "unknown"
            try:
                instrument_token = tick.get("instrument_token")
                symbol = self.token_to_symbol.get(instrument_token)
                if symbol is None:
                    continue

                record = {**tick, "symbol": symbol}
                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=json.dumps(record, default=json_default),
                    callback=self.delivery_report,
                )
                self.producer.poll(0)
            except Exception:
                self.logger.exception(
                    f"Failed to produce real-time tick for {symbol}"
                )

    def on_error(self, websocket, code, reason):
        self.logger.error(f"Kite WebSocket error {code}: {reason}")

    def on_close(self, websocket, code, reason):
        self.logger.warning(f"Kite WebSocket closed {code}: {reason}")

    def produce_stock_data(self):
        producer = None
        ticker = None
        try:
            kite = self.authenticate_kite()
            producer = self.create_kafka_producer()
            self.producer = producer
            self.token_to_symbol = self.get_instrument_tokens(kite)

            ticker = KiteTicker(self.api_key, self.access_token)
            ticker.on_connect = self.on_connect
            ticker.on_ticks = self.on_ticks
            ticker.on_error = self.on_error
            ticker.on_close = self.on_close

            self.logger.info("Starting real-time stock data production")
            ticker.connect()
        except KeyboardInterrupt:
            self.logger.info("Producer stopped by the user")
        finally:
            if ticker is not None:
                try:
                    ticker.close()
                except Exception:
                    self.logger.exception("Failed to close Kite WebSocket cleanly")

            if producer is not None:
                try:
                    producer.flush()
                except Exception:
                    self.logger.exception("Failed to flush Kafka producer cleanly")

            self.logger.info("Producer shut down")


def main():
    try:
        logger.info("Starting Real-Time Stock Data Collector")
        collector = StreamDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_REALTIME,
        )
        collector.produce_stock_data()
    except Exception:
        logger.exception("Fatal error in real-time stock data collector")
        raise


if __name__ == "__main__":
    main()
