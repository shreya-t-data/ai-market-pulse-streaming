import os
import json
import time
import yaml 
import logging
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POLL_INTERVAL_SECONDS = 300 # 5 minutes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s %(message)s]")
logger = logging.getLogger("news_producer")

with open("config/tickers.yaml") as f:
    TICKERS = yaml.safe_load(f)["tickers"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


seen_urls = set() # simple in-memory dedup cache


def fetch_news(ticker):
    today = time.strftime("%Y-%m-%d")
    yesterday = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))
    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker,
        "from": yesterday,
        "to": today,
        "token": FINNHUB_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def poll_once():
    new_count = 0
    for ticker in TICKERS:
        try:
            articles = fetch_news(ticker)
        except Exception as e:
            logger.error(f"Failed to fetch news fro {ticker}: {e}")
            continue
        
        for article in articles:
            article_url = article.get("url")
            if not article_url or article_url in seen_urls:
                continue
            seen_urls.add(article_url)

            record = {
                "ticker": ticker,
                "headline": article.get("headline"),
                "summary": article.get("summary"),
                "source": article.get("source"),
                "url": article_url,
                "published_at": article.get("datetime"),
                "received_at": int(time.time() * 1000)
            }
            producer.send("market-news", key=ticker, value=record)
            new_count += 1

        time.sleep(1) # pacing: stay well under 60 calls/min across tickers

    logger.info(f"Poll complete: {new_count} new headlines published")


if __name__ == "__main__":
    logger.info("Starting news producer (polling every 5 mins)")
    while True:
        poll_once()
        time.sleep(POLL_INTERVAL_SECONDS)

