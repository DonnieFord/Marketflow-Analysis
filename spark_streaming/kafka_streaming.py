from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

#Define schema for individual coin
coin_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True),
])

# Define schema for full message
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", ArrayType(coin_schema), True)
    ])

# Create SparkSession
spark = SparkSession.builder \
.appName("kafkaCryptoConsumer") \
.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from kafka topic - from beginning
raw_df = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "crypto-prices") \
.option("startingOffsets", "earliest") \
.load()

# Convert kafka value from ytes to string
json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
# Parse JSON
parsed_df = json_df.select(from_json(col("json"), schema).alias("parsed"))

# Flatten structure
flattened_df = parsed_df.select(
    col("parsed.timestamp").alias("timestamp"),
    explode(col("parsed.data")).alias("coin")
    ).select(
        "timestamp",
        col("coin.id").alias("id"),
        col("coin.symbol").alias("symbol"),
        col("coin.current_price").alias("price"),
        col("coin.market_cap"),
        col("coin.total_volume"),
        col("coin.high_24h"),
        col("coin.low_24h"),
        col("coin.last_updated")
    )

# Define the output path for the parquet files
output_path = "file:///C:/Users/Donni/OneDrive/Desktop/Marketflow-Analysis/dataframes"
checkpoint_path = "file:///C:/Users/Donni/OneDrive/Desktop/Marketflow-Analysis/checkpoints"

# Write to parquet files in the specified folder
query = flattened_df.writeStream \
.format("parquet") \
.outputMode("append") \
.trigger(processingTime="30 seconds") \
.option("path", output_path) \
.option("checkpointLocation", checkpoint_path) \
.start()

print("Streaming query started.", flush=True)

while query.isActive:
    print("Status:", query.status, flush=True)

    progress = query.lastProgress
    if progress is not None:
        print(
            f"Latest completed batch: {progress['batchId']} | "
            f"Kafka messages read: {progress['numInputRows']}",
            flush=True
        )
    else:
        print("No completed batch yet.", flush=True)

    if query.awaitTermination(10):
        break