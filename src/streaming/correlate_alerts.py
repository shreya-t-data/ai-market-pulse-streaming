from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StringType, DoubleType, LongType


spark = SparkSession.builder.appName("CorrelateAlerts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


trade_schema = StructType() \
    .add("ticker", StringType()).add("price", DoubleType()) \
    .add("volume", LongType()).add("trade_timestamp", LongType()).add("received_at", LongType())


news_schema = StructType() \
    .add("ticker", StringType()).add("headline", StringType()).add("summary", StringType()) \
    .add("source", StringType()).add("url", StringType()) \
    .add("published_at", LongType()).add("received_at", LongType())


trades = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-trades").option("startingOffsets", "latest").load() \
    .select(from_json(col("value").cast("string"), trade_schema).alias("d")).select("d.*") \
    .withColumn("trade_time", to_timestamp(col("trade_timestamp") / 1000)) \
    .withWatermark("trade_time", "2 minutes")


news = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-news").option("startingOffsets", "earliest").load() \
    .select(from_json(col("value").cast("string"), news_schema).alias("d")).select("d.*") \
    .withColumn("news_time", to_timestamp(col("published_at"))) \
    .withWatermark("news_time", "20 minutes")


joined = trades.alias("t").join(
    news.alias("n"),
    expr("""
    t.ticker = n.ticker AND
    n.news_time >= t.trade_time - interval 15 minutes AND
    n.news_time <= t.trade_time
    """)
)


alerts = joined.select(
    col("t.ticker").alias("ticker"),
    col("t.price").alias("price"),
    col("n.headline").alias("headline"),
    col("t.trade_time").alias("event_time")
)


def write_alerts(batch_df, batch_id):
    count = batch_df.count()
    print(f">>> Alert batch {batch_id}, row count: {count}")
    if count > 0:
        batch_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/market_pulse") \
            .option("dbtable", "gold.market_alerts") \
            .option("user", "shreya").option("password", "changeme123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append").save()


query = alerts.writeStream.foreachBatch(write_alerts) \
    .option("checkpointLocation", "./checkpoints/correlate_alerts").start()


query.awaitTermination()