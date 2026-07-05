import os
import json
import time
import yaml
import logging
import threading
import websocket
import random
from datetime import datetime, time as dtime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("stock_trade_producer")

#Load tickers
with open("config/tickers.yaml") as f:
    TICKERS = yaml.safe_load(f)["tickers"]

#Kafka producer, keyed by ticker
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

message_count = 0
count_lock = threading.Lock()

def log_throughput():
    global message_count
    while True:
        time.sleep(30)
        with count_lock:
            logger.info(f"Sent {message_count} messages in the last 30s")
            message_count = 0


def on_message(ws, message):
    global message_count
    data = json.loads(message)
    if data.get("type") != "trade":
        return
    for trade in data.get("data", []):
        record = {
            "ticker": trade["s"],
            "price": trade["p"],
            "volume": trade["v"],
            "trade_timestamp": trade["t"],
            "received_at": int(time.time() * 1000),
        }
        producer.send("stock-trades", key=record["ticker"], value=record)
        with count_lock:
            message_count +=1


def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.warning("WebSocket closed. Reconnecting in 5s...")
    time.sleep(5)
    start_websocket()


def on_open(ws):
    logger.info("WebSocket connected. Subscribing to tickers...")
    for ticker in TICKERS:
        ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))


def start_websocket():
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()


def market_is_open():
    now = datetime.utcnow()
    #US market hours: 9:30-16:00 ET = 13:30-20:00 UTC (ignoring DST edge cases)
    if now.weekday() >= 5:
        return False
    current = now.time()
    return dtime(13, 30) <= current <= dtime(20, 0)


def replay_mode():
    logger.info("Market closed - running in REPLAY mode with simulated trades")
    base_prices = {t: random.uniform(100, 500) for t in TICKERS}
    global message_count
    while True:
        ticker = random.choice(TICKERS)
        base_prices[ticker] *= (1 + random.uniform(-0.002, 0.002))
        record = {
            "ticker": ticker,
            "price": round(base_prices[ticker], 2),
            "volume": random.randint(1, 500),
            "trade_timestamp": int(time.time() * 1000),
            "received_at": int(time.time() * 1000),
        }
        producer.send("stock-trades", key=record["ticker"], value=record)
        with count_lock:
            message_count += 1
            time.sleep(random.uniform(0.1, 1.5))



if __name__ == "__main__":
    threading.Thread(target=log_throughput, daemon=True).start()
    if market_is_open():
        start_websocket()
    else:
        replay_mode()

