import os 
import time 
import logging
import requests
import psycopg2
from dotenv import load_dotenv


load_dotenv()


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("alert_watcher")


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL") #optional
POLL_INTERVAL = 30


conn = psycopg2.connect(
    host="localhost", port=5432, dbname="market_pulse",
    user="shreya", password="changeme123"
)
conn.autocommit = True

last_seen_ctid = None


def format_alert(row):
    ticker, price, headline, event_time = row
    return f"🚨 {ticker} @ ${price:.2f} - possible news-driven move: \"{headline}\" ({event_time})"


def send_slack(message):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=5)
    except Exception as e:
        logger.error(f"Slack webhook failed: {e}")


def poll():
    global last_seen_ctid
    cur = conn.cursor()
    if last_seen_ctid is None:
        cur.execute("SELECT ctid, ticker, price, headline, event_time FROM gold.market_alerts ORDER BY event_time DESC LIMIT 1")
        row = cur.fetchone()
        last_seen_ctid = row[0] if row else None
        cur.close()
        return

    cur.execute("SELECT ctid, ticker, price, headline, event_time FROM gold.mnarket_alerts ORDER BY event_time DESC LIMIT 20")
    rows = cur.fetchall()
    cur.close()

    for row in reversed(rows):
        ctid, ticker, price, headline, event_time = row
        message = format_alert((ticker, price, headline, event_time))
        logger.info(message)
        send_slack(message)

    if rows:
        last_seen_ctid = rows[0][0]


if __name__ == "__main__":
    logger.info("Alert watcher started (polling every 30s)")
    if not SLACK_WEBHOOK_URL:
        logger.info("NO SLACK_WEBHOOK_URL set - console-only mode")
    while True:
        poll()
        time.sleep(POLL_INTERVAL)