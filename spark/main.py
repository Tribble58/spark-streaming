import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, to_date, sum, count, window, expr, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:9092"
STATIC_DATA_DIR = "/data/static"
DATA_DIR = "/data/output"
CHECKPOINT_DIR = "/tmp/checkpoint"

if __name__ == '__main__':
    try:
        spark = (SparkSession
                 .builder
                 .appName("final_project")
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR")

        users_schema = StructType(
            [
                StructField("user_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("last_name", StringType(), False),
                StructField("city", StringType(), False),
                StructField("phone", StringType(), False)
            ]
        )

        # Load static table
        df_users = (spark
                    .read
                    .format("csv")
                    .option("header", True)
                    .schema(users_schema)
                    .load(STATIC_DATA_DIR + "/csv")
                    )

        # Define clicks message schema
        clicks_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_time", TimestampType(), False),
                StructField("user_id", IntegerType(), False),
                StructField("action", StringType(), False),
                StructField("value", IntegerType(), False)
            ]
        )

        # Define purchases message schema
        purchases_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_time", TimestampType(), False),
                StructField("user_id", IntegerType(), False),
                StructField("action", StringType(), False),
                StructField("value", IntegerType(), False)
            ]
        )

        # Read clicks
        df_clicks = (
            spark
            .readStream
            .format("kafka")
            .option("subscribe", "stream-clicks")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .load()
        )

        df_clicks_parsed = (
            df_clicks
            .select(
                from_json(
                    col("value").cast("string"),
                    clicks_schema
                ).alias("json_data"),
                col("timestamp").alias("processing_ts")
            )
            .select("json_data.event_id",
                    col("json_data.event_time").alias("click_ts"),
                    "json_data.user_id",
                    "json_data.value",
                    "processing_ts",
                    # For further partitioning
                    to_date("processing_ts").alias("processing_date"))
        )

        # Read purchases
        df_purchases = (
            spark
            .readStream
            .format("kafka")
            .option("subscribe", "stream-purchases")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .load()
        )

        df_purchases_parsed = (
            df_purchases
            .select(
                from_json(
                    col("value").cast("string"),
                    clicks_schema
                ).alias("json_data"),
                col("timestamp").alias("processing_ts")
            )
            .select("json_data.event_id",
                    col("json_data.event_time").alias("purchase_ts"),
                    "json_data.user_id",
                    "json_data.value",
                    "processing_ts",
                    # For further partitioning
                    to_date("processing_ts").alias("processing_date"))
        )

        # Clicks enrichment with users data
        stream_clicks_enrich = (
            df_clicks_parsed
            .join(
                df_users,
                ["user_id"],
                "left"
            )
            .writeStream
            .format("parquet")
            .option("path", DATA_DIR + "/parquet/clicks_enriched")
            .option("checkpointLocation", CHECKPOINT_DIR + "/parquet/clicks_enriched")
            .partitionBy("processing_date")
            .outputMode("append")
            .start()
        )

        df_clicks_watermark = (
            df_clicks_parsed
            .withWatermark("click_ts", "20 seconds")
        )

        df_purchases_watermark = (
            df_purchases_parsed
            .withWatermark("purchase_ts", "20 seconds")
        )

        # Joining clicks with purchases
        df_attributed_purchases = (
            df_purchases_watermark.alias("p")
            .join(
                df_clicks_watermark.alias("c"),
                (df_purchases_watermark.user_id == df_clicks_watermark.user_id) &
                (df_purchases_watermark.purchase_ts >= df_clicks_watermark.click_ts) &
                (df_purchases_watermark.purchase_ts < df_clicks_watermark.click_ts + expr("interval 1 minute")),
                "left"
            )
            .select("p.event_id",
                    "p.user_id",
                    "p.value",
                    col("c.event_id").alias("click_id"),
                    col("c.click_ts").alias("click_time")
                    )
        )

        # Write attributed purchases to Kafka
        stream_attributed_purchases_kafka = (
            df_attributed_purchases
            .select(to_json
                    (struct
                     (col("event_id"),
                      col("user_id"),
                      col("value"),
                      col("click_id"),
                      col("click_time")
                      )).alias("value")
                    )
            .writeStream
            .queryName("stream_attributed_purchases_kafka")
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", "attributed-purchases")
            .option("checkpointLocation", CHECKPOINT_DIR + "/kafka/attributed_purchases")
            .outputMode("append")
            .start()
        )

        # Write attributed purchases to console
        stream_attributed_purchases_console = (
            df_attributed_purchases
            .writeStream
            .queryName("stream_attributed_purchases_console")
            .format("console")
            .option("truncate", False)
            .option("topic", "attributed-purchases")
            .option("checkpointLocation", CHECKPOINT_DIR + "/console/attributed_purchases")
            .outputMode("append")
            .start()
        )

        # Aggregate purchases by window and write to parquet partitioned by processing_date
        stream_aggregated_purchases = (
            df_purchases_watermark
            .groupBy("processing_date", window("purchase_ts", "10 seconds", "5 seconds"))
            .agg(
                count("purchase_ts").alias("purchases_cnt"),
                sum("value").alias("purchases_sum")
            )
            .writeStream
            .format("parquet")
            .option("checkpointLocation", CHECKPOINT_DIR + "/parquet/aggregated_purchases")
            .option("path", DATA_DIR + "/parquet/aggregated_purchases")
            .partitionBy("processing_date")
            .outputMode("append")
            .start()
        )

        # Drop purchase duplicate data and write it to parquet partitioned by processing_date
        stream_deduplicated_clicks = (
            df_clicks_watermark
            .dropDuplicates(["event_id"])
            .writeStream
            .format("parquet")
            .option("checkpointLocation", CHECKPOINT_DIR + "/parquet/deduplicated_clicks")
            .option("path", DATA_DIR + "/parquet/deduplicated_clicks")
            .outputMode("append")
            .partitionBy("processing_date")
            .start()
        )

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.info(f"Error: {e}")
