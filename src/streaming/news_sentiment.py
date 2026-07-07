from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, pandas_udf
from pyspark.sql.types import StructType, StringType, LongType, DoubleType
import pandas as pd 
from nltk.sentiment import SentimentIntensityAnalyzer


spark = SparkSession.builder \
    .appName("NewsSentimentScoring") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")


news_schema = StructType() \
    .add("ticker", StringType()) \
    .add("headline", StringType()) \
    .add("summary", StringType()) \
    .add("source", StringType()) \
    .add("url", StringType()) \
    .add("published_at", LongType()) \
    .add("received_at", LongType()) \


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-news") \
    .option("startingOffsets", "latest") \
    .load()


parsed_df = raw_df.select(
    from_json(col("value").cast("string"), news_schema).alias("data")
).select("data.*")


# Pandas UDF: scores a whole batch of headlines+summaries at once
@pandas_udf(DoubleType())
def score_sentiment(headlines: pd.Series, summaries: pd.Series) -> pd.Series:
    sia = SentimentIntensityAnalyzer()
    combined = headlines.fillna("") + ". " + summaries.fillna("")
    return combined.apply(lambda text: sia.polarity_scores(text)["compound"])


scored_df = parsed_df.withColumn(
    "sentiment_score", score_sentiment(col("headline"), col("summary"))
)


def write_to_postgres(batch_df, batch_id):
    print(f">>> Sentiment batch {batch_id}, row count: {batch_df.count()}")
    batch_df.select(
        "ticker", "headline", "sentiment_score", "published_at"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/market_pulse") \
        .option("dbtable", "silver.news_sentiment") \
        .option("user", "shreya") \
        .option("password", "changeme123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


query = scored_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./checkpoints/news_sentiment") \
    .start()


query.awaitTermination()
