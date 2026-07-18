"""Generate a Kite access token for manual entry into the project .env file.

Run from the repository root:
    python src/utilities/generate_access_token.py
"""

import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from kiteconnect import KiteConnect


ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


def request_token_from_input(value: str) -> str:
    value = value.strip()
    if "request_token=" in value:
        query = urlparse(value).query or value.lstrip("?")
        return parse_qs(query).get("request_token", [""])[0]
    return value


def main() -> None:
    load_dotenv(ENV_FILE)
    api_key = os.getenv("KITE_API_KEY")
    api_secret = os.getenv("KITE_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit("Set KITE_API_KEY and KITE_API_SECRET in .env first.")

    kite = KiteConnect(api_key=api_key)
    print(f"Open this URL and log in:\n{kite.login_url()}")
    request_token = request_token_from_input(
        input("Paste the request_token or full redirect URL: ")
    )
    if not request_token:
        raise SystemExit("No request_token found in the input.")

    session = kite.generate_session(request_token, api_secret=api_secret)
    print(f"\nKITE_ACCESS_TOKEN={session['access_token']}")
    print("Paste this line into your local .env file and keep the token private.")


if __name__ == "__main__":
    main()
