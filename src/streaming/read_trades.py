from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, first, last, max as spark_max,
    min as spark_min, sum as spark_sum, count, to_timestamp
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("StockOHLCAggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Schema matching our stock-trades JSON (from schemas.md)
trade_schema = StructType() \
    .add("ticker", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", LongType()) \
    .add("trade_timestamp", LongType()) \
    .add("received_at", LongType()) 


# Read raw stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-trades") \
    .option("startingOffsets", "latest") \
    .load()


#Kafka gives us key/value as bytes - parse value as JSON
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), trade_schema).alias("data")
).select("data.*")


# Convert epoch ms -> Spark timestamp (required for windowing/watermarking)
trades_with_time = parsed_df.withColumn(
    "event_time", to_timestamp(col("trade_timestamp") / 1000)
)


# Watermark: tolerate up to 2 minutes of late-arriving data
watermarked = trades_with_time.withWatermark("event_time", "2 minutes")


# 1-minute tumbling window, grouped per ticker
ohlc = watermarked.groupBy(
    window(col("event_time"), "1 minute"),
    col("ticker")
).agg(
    first("price").alias("open"),
    spark_max("price").alias("high"),
    spark_min("price").alias("low"),
    last("price").alias("close"),
    spark_sum("volume").alias("total_volume"),
    count("*").alias("trade_count")
).select(
    col("ticker"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "open", "high", "low", "close", "total_volume", "trade_count"
)


#Write to postgres
def write_to_postgres(batch_df, batch_id):
    print(f">>> Processing batch {batch_id}, row count: {batch_df.count()}")
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/market_pulse") \
        .option("dbtable", "gold.stock_ohlc_1min_staging") \
        .option("user", "shreya") \
        .option("password", "changeme123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()


    # Upsert from staging into real table
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="market_pulse",
        user="shreya", password="changeme123"
    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gold.stock_ohlc_1min (ticker, window_start, window_end, open, high, low, close, total_volume, trade_count, updated_at)
        SELECT ticker, window_start, window_end, open, high, low, close, total_volume, trade_count, now()
        FROM gold.stock_ohlc_1min_staging
        ON CONFLICT (ticker, window_start)
        DO UPDATE SET
            window_end = EXCLUDED.window_end,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            total_volume = EXCLUDED.total_volume,
            trade_count = EXCLUDED.trade_count,
            updated_at = now();
    """)
    conn.commit()
    cur.close()
    conn.close()


query = ohlc.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./checkpoints/stock_ohlc") \
    .start()


query.awaitTermination()
