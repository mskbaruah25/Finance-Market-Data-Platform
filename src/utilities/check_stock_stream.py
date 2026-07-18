"""Check whether Kite WebSocket ticks arrive for an NSE stock.

Run after setting KITE_ACCESS_TOKEN in .env:
    python src/utilities/check_stock_stream.py --symbol RELIANCE
"""

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path
from threading import Event

from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker


ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def main() -> None:
    parser = argparse.ArgumentParser(description="Print live Kite ticks for one NSE stock.")
    parser.add_argument("--symbol", default="RELIANCE", help="NSE trading symbol (default: RELIANCE)")
    parser.add_argument("--max-ticks", type=int, default=5, help="Stop after this many ticks (default: 5)")
    parser.add_argument("--timeout", type=float, default=60, help="Stop after this many seconds (default: 60)")
    args = parser.parse_args()
    if args.max_ticks < 1 or args.timeout <= 0:
        parser.error("--max-ticks and --timeout must be positive")

    load_dotenv(ENV_FILE)
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        raise SystemExit("Set KITE_API_KEY and KITE_ACCESS_TOKEN in .env first.")

    symbol = args.symbol.upper().removesuffix(".NS")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    instrument = next(
        (item for item in kite.instruments("NSE") if item["tradingsymbol"] == symbol),
        None,
    )
    if instrument is None:
        raise SystemExit(f"NSE symbol {symbol!r} was not found in Kite's instrument list.")
    token = int(instrument["instrument_token"])

    done = Event()
    result = {"ticks": 0, "error": None}
    ticker = KiteTicker(api_key, access_token)

    def on_connect(ws, response):
        ws.subscribe([token])
        ws.set_mode(ws.MODE_QUOTE, [token])
        print(f"Subscribed to NSE:{symbol} (instrument token {token}). Waiting for ticks...", flush=True)

    def on_ticks(ws, ticks):
        for tick in ticks:
            if tick.get("instrument_token") != token:
                continue
            print(json.dumps({**tick, "symbol": f"NSE:{symbol}"}, default=json_default), flush=True)
            result["ticks"] += 1
            if result["ticks"] >= args.max_ticks:
                done.set()
                break

    def on_error(ws, code, reason):
        result["error"] = f"Kite WebSocket error {code}: {reason}"
        done.set()

    def on_noreconnect(ws):
        result["error"] = "Kite WebSocket could not reconnect."
        done.set()

    ticker.on_connect = on_connect
    ticker.on_ticks = on_ticks
    ticker.on_error = on_error
    ticker.on_noreconnect = on_noreconnect

    try:
        ticker.connect(threaded=True)
        done.wait(args.timeout)
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        ticker.close()

    if result["error"]:
        raise SystemExit(result["error"])
    if result["ticks"] == 0:
        raise SystemExit("No live ticks received. Try again during NSE market hours.")
    print(f"Received {result['ticks']} live tick(s) for NSE:{symbol}.")


if __name__ == "__main__":
    main()
