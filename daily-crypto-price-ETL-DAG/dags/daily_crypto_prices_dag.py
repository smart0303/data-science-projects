"""Daily ETL: fetch top crypto prices from CoinGecko, save CSV, print summary."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

default_args = {
    "owner": "data-science",
    "retries": 1,
}

with DAG(
    dag_id="daily_crypto_prices",
    default_args=default_args,
    description="Fetch top 10 crypto prices daily, save to CSV, print summary",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "etl"],
) as dag:

    @task
    def fetch_crypto_prices() -> list[dict]:
        response = requests.get(API_URL, params=API_PARAMS, timeout=30)
        response.raise_for_status()
        coins = response.json()
        return [
            {
                "id": coin["id"],
                "symbol": coin["symbol"],
                "name": coin["name"],
                "current_price": coin.get("current_price"),
                "market_cap": coin.get("market_cap"),
                "market_cap_rank": coin.get("market_cap_rank"),
                "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                "last_updated": coin.get("last_updated"),
            }
            for coin in coins
        ]

    @task
    def save_to_csv(coins: list[dict]) -> str:
        context = get_current_context()
        run_date = context["ds"]

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        filepath = DATA_DIR / f"crypto_prices_{run_date}.csv"

        if not coins:
            raise ValueError("No coin data returned from API")

        fieldnames = list(coins[0].keys())
        with filepath.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(coins)

        return str(filepath)

    @task
    def print_summary(coins: list[dict], csv_path: str) -> None:
        prices = [c["current_price"] for c in coins if c.get("current_price") is not None]

        print("=== Crypto Prices Summary ===")
        print(f"  rows_saved: {len(coins)}")
        print(f"  csv_path: {csv_path}")
        if coins:
            print(f"  top_coin: {coins[0]['name']} ({coins[0]['symbol'].upper()})")
            print(f"  top_price_usd: ${coins[0].get('current_price', 0):,.2f}")
        if prices:
            print(f"  avg_price_usd: ${sum(prices) / len(prices):,.2f}")
            print(f"  min_price_usd: ${min(prices):,.2f}")
            print(f"  max_price_usd: ${max(prices):,.2f}")

        print("=== Top 10 by Market Cap ===")
        for coin in coins:
            rank = coin.get("market_cap_rank") or "?"
            price = coin.get("current_price") or 0
            symbol = coin["symbol"].upper()
            print(f"  {rank:>2}. {coin['name']} ({symbol}): ${price:,.2f}")

    coin_data = fetch_crypto_prices()
    output_path = save_to_csv(coin_data)
    print_summary(coin_data, output_path)
