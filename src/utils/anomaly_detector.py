import time
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("anomaly_detector")

CHECK_INTERVAL = 60  # seconds

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="market_pulse",
    user="shreya", password="changeme123"
)
conn.autocommit = True


def check_anomalies():
    cur = conn.cursor()
    # For each ticker, compute pct move per window, then z-score vs trailing 30-min stats
    cur.execute("""
        WITH moves AS (
            SELECT
                ticker,
                window_start,
                close,
                (close - open) / NULLIF(open, 0) * 100 AS pct_move
            FROM gold.stock_ohlc_1min
            WHERE window_start >= now() - interval '35 minutes'
        ),
        stats AS (
            SELECT
                ticker,
                AVG(pct_move) AS avg_move,
                STDDEV(pct_move) AS stddev_move
            FROM moves
            WHERE window_start < now() - interval '1 minute'
            GROUP BY ticker
        ),
        latest AS (
            SELECT DISTINCT ON (ticker) ticker, window_start, close, pct_move
            FROM moves
            ORDER BY ticker, window_start DESC
        )
        SELECT lt.ticker, lt.close, lt.pct_move, s.avg_move, s.stddev_move,
               (lt.pct_move - s.avg_move) / NULLIF(s.stddev_move, 0) AS z_score
        FROM latest lt
        JOIN stats s ON lt.ticker = s.ticker
        WHERE s.stddev_move > 0
          AND ABS((lt.pct_move - s.avg_move) / NULLIF(s.stddev_move, 0)) > 2;
    """)
    rows = cur.fetchall()


    for ticker, close, pct_move, avg_move, stddev_move, z_score in rows:
        logger.info(f"⚠️  ANOMALY: {ticker} moved {pct_move:.2f}% (z-score {z_score:.2f}) — price ${close:.2f}")
        cur.execute("""
            INSERT INTO gold.market_alerts (ticker, price, headline, event_time, alert_type)
            VALUES (%s, %s, %s, now(), 'statistical_anomaly')
        """, (ticker, close, f"Statistical anomaly: {pct_move:.2f}% move (z-score {z_score:.2f})"))

    cur.close()


if __name__ == "__main__":
    logger.info("Anomaly detector started (checking every 60s)")
    while True:
        check_anomalies()
        time.sleep(CHECK_INTERVAL)