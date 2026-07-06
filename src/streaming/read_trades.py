from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col 
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("ReadStockTrades") \
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


#Write to console for debugging
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
