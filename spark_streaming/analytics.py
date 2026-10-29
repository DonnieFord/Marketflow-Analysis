import time
import traceback
import psycopg2
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, avg, stddev, lag,
    round, when, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName("CryptoMetricsCalculator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# PostgreSQL connection
jdbc_url = "jdbc:postgresql://localhost:5432/crypto_metrics"
main_table = "crypto_table"
gainers_table = "top_5_gainers"
losers_table = "top_5_losers"

db_properties = {
    "user": "postgres",
    "password": os.environ["CRYPTO_DB_PASSWORD"],
    "driver": "org.postgresql.Driver"
}


if __name__ == "__main__":
    try:
        print("\n=== Running Spark Job ===")

        # Load Parquet data
        df = spark.read.parquet(
            "file:///C:/Users/Donni/OneDrive/Desktop/Marketflow-Analysis/dataframes"
        ).drop(
            "market_cap", "total_volume",
            "high_24h", "low_24h", "last_updated"
        )

        # Convert data types
        df = df.withColumn(
            "timestamp", to_timestamp("timestamp")
        ).withColumn(
            "price", col("price").cast(DoubleType())
        )

        # Filter recent data
        latest_data = df.filter(
            col("timestamp") >=
            F.current_timestamp() - F.expr("INTERVAL 13 MINUTES")
        )

        row_count = latest_data.count()

        if row_count == 0:
            print("No data found in the last 13 minutes.")

        else:
            # Window for each cryptocurrency
            coin_window = Window.partitionBy("id").orderBy("timestamp")
            time_window = Window.partitionBy("id").orderBy(
            col("timestamp").cast("long")
            )

            one_minute_back = time_window.rangeBetween(
            Window.unboundedPreceding, -60
            )

            five_minutes_back = time_window.rangeBetween(
              Window.unboundedPreceding, -300
             )
            # Price changes
            latest_data = (
                latest_data
                .withColumn(
                    "price_1min_ago",
                    F.last("price", ignorenulls=True).over(one_minute_back)
                )
                .withColumn(
                    "price_5min_ago",
                    F.last("price", ignorenulls=True).over(five_minutes_back)
                )
                .withColumn(
                    "change_1min",
                    when(
                        col("price_1min_ago").isNull() |
                        (col("price_1min_ago") == 0),
                        None
                    ).otherwise(
                        round(
                            (col("price") - col("price_1min_ago"))
                            / col("price_1min_ago") * 100,
                            2
                        )
                    )
                )
                .withColumn(
                    "change_5min",
                    when(
                        col("price_5min_ago").isNull() |
                        (col("price_5min_ago") == 0),
                        None
                    ).otherwise(
                        round(
                            (col("price") - col("price_5min_ago"))
                            / col("price_5min_ago") * 100,
                            2
                        )
                    )
                )
                .drop("price_1min_ago", "price_5min_ago")
            )

            # Rolling averages
                        # Five-observation simple moving average
            latest_data = latest_data.withColumn(
                "SMA",
                avg("price").over(coin_window.rowsBetween(-4, 0))
            )

            # Collect each coin's available history in timestamp order
            ema_window = coin_window.rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )

            latest_data = latest_data.withColumn(
                "_ema_history",
                F.sort_array(
                    F.collect_list(
                        F.struct("timestamp", "price")
                    ).over(ema_window)
                )
            )

            # Three-observation EMA: alpha = 2 / (3 + 1) = 0.5
            latest_data = latest_data.withColumn(
                "EMA",
                F.aggregate(
                    col("_ema_history"),
                    F.lit(None).cast("double"),
                    lambda previous, point: (
                        F.when(
                            point["price"].isNull(), previous
                        )
                        .when(
                            previous.isNull(), point["price"]
                        )
                        .otherwise(
                            0.5 * point["price"] + 0.5 * previous
                        )
                    )
                )
            ).drop("_ema_history")

            # Volatility: corrected column spelling
            latest_data = latest_data.withColumn(
                "volatility",
                stddev("price").over(coin_window.rowsBetween(-4, 0))
            ) 
            # Keep the latest 7 minutes only after calculating metrics
            latest_data = latest_data.filter(
            col("timestamp") >=
            F.current_timestamp() - F.expr("INTERVAL 7 MINUTES")
            )
            latest_data = latest_data.cache()
            if latest_data.isEmpty():
                raise RuntimeError(
                    "No records remain in the 7-minute output window. "
                    "Check that the producer and streaming consumer are running."
                )

            # Keep only the newest record for each cryptocurrency
            latest_coin_window = Window.partitionBy("id").orderBy(
            col("timestamp").desc()
      )

            ranking_data = (
            latest_data
            .withColumn("latest_rank", row_number().over(latest_coin_window))
            .filter(col("latest_rank") == 1)
            .drop("latest_rank")
)
            # Top 5 gainers
            global_rank_gain = Window.orderBy(
            col("change_5min").desc_nulls_last(),
            col("id").asc()
           )

            gain_df = (
                ranking_data
                .filter(col("change_5min") > 0)
                .withColumn(
                    "rank",
                    row_number().over(global_rank_gain)
                )
                .filter(col("rank") <= 5)
                .select("rank", "id", "symbol")
            )

            # Top 5 losers
            global_rank_loss = Window.orderBy(
            col("change_5min").asc_nulls_last(),
            col("id").asc()
            )

            loss_df = (
                ranking_data
                .filter(col("change_5min") < 0)
                .withColumn(
                    "rank",
                    row_number().over(global_rank_loss)
                )
                .filter(col("rank") <= 5)
                .select("rank", "id", "symbol")
            )

            # Display results
            print("\n=== All Metrics ===")
            latest_data.select(
                "timestamp", "id", "symbol", "price",
                "change_1min", "change_5min",
                "SMA", "EMA", "volatility"
            ).orderBy("timestamp").show(truncate=False)

            print("\n=== Top 5 Gainers ===")
            gain_df.show(truncate=False)

            print("\n=== Top 5 Losers ===")
            loss_df.show(truncate=False)

            # Write results to PostgreSQL
        
            metric_columns = [
                "timestamp", "id", "symbol", "price",
                "change_1min", "change_5min",
                "SMA", "EMA", "volatility"
            ]

            upsert_sql = """
                INSERT INTO public.crypto_table_v2 (
                    "timestamp", "id", "symbol", "price",
                    "change_1min", "change_5min",
                    "SMA", "EMA", "volatility"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("id", "timestamp")
                DO UPDATE SET
                    "symbol" = EXCLUDED."symbol",
                    "price" = EXCLUDED."price",
                    "change_1min" = EXCLUDED."change_1min",
                    "change_5min" = EXCLUDED."change_5min",
                    "SMA" = EXCLUDED."SMA",
                    "EMA" = EXCLUDED."EMA",
                    "volatility" = EXCLUDED."volatility"
            """

            connection = psycopg2.connect(
                host="localhost",
                port=5432,
                dbname="crypto_metrics",
                user=db_properties["user"],
                password=db_properties["password"]
            )

            try:
                with connection:
                    with connection.cursor() as cursor:
                        for row in latest_data.select(
                            *metric_columns
                        ).toLocalIterator():
                            cursor.execute(upsert_sql, tuple(row))
            finally:
                connection.close()

            print("Main metrics saved to crypto_table_v2.")

            gain_df.write.jdbc(
                url=jdbc_url,
                table=gainers_table,
                mode="overwrite",
                properties=db_properties
            )

            loss_df.write.jdbc(
                url=jdbc_url,
                table=losers_table,
                mode="overwrite",
                properties=db_properties
            )

            print("Data written to PostgreSQL (all 3 tables).")
        spark.catalog.clearCache()

    except Exception as e:
        print(f"Error in Spark job: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()
