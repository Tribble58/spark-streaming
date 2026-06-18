import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, to_date, sum, count, window, expr

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

static_data_dir = "/data/static"
data_dir = "/data/spark_output"
checkpoint_dir = "/tmp/checkpoint"

if __name__ == '__main__':
    try:
        spark = (SparkSession
                 .builder
                 .appName("tutorial")
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR")

        users_schema = "user_id int, name string, last_name string, city string, phone string"

        df_users = spark.read.format("csv").option("header", True).schema(users_schema).load(static_data_dir)

        df_users.cache()

        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stream-input") \
            .option("failOnDataLoss", "false") \
            .load()

        # Define DDL
        kafka_message_schema = "event_id string, event_time timestamp, user_id int, action string, value int"

        df_parsed = (df
                     .select(from_json(col("value").cast("string"),
                                       kafka_message_schema)
                             # required alias for further selecting
                             .alias("json_data"),
                             "offset",
                             col("timestamp").alias("processing_ts"),
                             )
                     .select("json_data.event_id", "json_data.user_id", "json_data.action", "json_data.value",
                             col("json_data.event_time").alias("event_ts"), "offset", "processing_ts",
                             to_date(col("processing_ts")).alias("processing_date")))

        query_parquet = (df_parsed
                         .filter(col("action") == "click")
                         .writeStream
                         .trigger(processingTime="5 seconds")
                         # Output result to parquet
                         .format("parquet")
                         .option("path", data_dir + "/parquet")
                         .option("checkpointLocation", checkpoint_dir + "/parquet")
                         .partitionBy("processing_date")
                         .outputMode("append")
                         .start()
                         )

        query_kafka = (df_parsed
                       .filter(col("action") == "click")
                       .select(to_json
                               (struct
                                (col("event_id"),
                                 col("user_id"),
                                 col("value"),
                                 col("event_ts")
                                 )).alias("value")
                               )
                       .writeStream
                       .trigger(processingTime="5 seconds")
                       # Output result to kafka
                       .format("kafka")
                       .option("kafka.bootstrap.servers", "kafka:9092")
                       .option("topic", "stream-output")
                       .option("checkpointLocation", checkpoint_dir + "/kafka")
                       .outputMode("append")
                       .start()
                       )

        # Count number of clicks made by user
        # query_console = (df
        #                      .select(from_json(col("value").cast("string"),
        #                                        kafka_message_schema)
        #                              # required alias for further selecting
        #                              .alias("json_data"),
        #                              "offset",
        #                              col("timestamp").alias("processing_ts"),
        #                              )
        #                      .select("json_data.event_id", "json_data.user_id", "json_data.action", "json_data.value",
        #                              col("json_data.event_time").alias("event_ts"), "offset",
        #                              to_date(col("processing_ts")).alias("processing_date"))
        #                      .filter(col("action") == "click")
        #                  .select("json_data.user_id", "json_data.action", "json_data.value")
        #                  .filter(col("action") == "click")
        #                  .groupby("user_id")
        #                  # .count()
        #                  .agg(sum("value"),
        #                       count("user_id").alias("count"))
        #                  # Sorting is only available in complete mode
        #                  .orderBy("count", ascending=False)
        #                  .writeStream
        #                  .trigger(processingTime="5 seconds")
        #                  # Output result to console
        #                  .format("console")
        #                  .outputMode("complete")
        #                  .option("checkpointLocation", stateful_transformations_checkpoint_dir)
        #                  .start()
        #                  )

        query_parquet_watermark = (
            df_parsed
            .filter(col("action") == "click")
            .select("user_id", "action", "value", "event_ts", to_date(col("event_ts")).alias("event_dt"))
            .withWatermark("event_ts", "10 seconds")
            .groupBy(window("event_ts", "10 seconds", "5 seconds"))
            .agg(
                count("user_id").alias("count_in_window"),
                sum("value").alias("sum_in_window")
            )
            # .withColumn("event_dt", to_date(col("window.start")))
            .writeStream
            # Output result to parquet
            .format("parquet")
            .option("path", data_dir + "/parquet_watermark")
            .option("checkpointLocation", checkpoint_dir + "/parquet_watermark")
            # .partitionBy("event_dt")
            .outputMode("append")
            .start()
        )

        # Stream-static join
        query_stream_static_join = (
            df_parsed
            .join(df_users, how="left", on="user_id")
            .select("event_id", "user_id", "name", "last_name", "city", "phone", "action", "value", "event_ts",
                    "processing_ts")
            .writeStream
            # Output result to parquet
            .format("parquet")
            .option("path", data_dir + "/parquet_static_join")
            .option("checkpointLocation", checkpoint_dir + "/parquet_static_join")
            .outputMode("append")
            .start()
        )

        # Read another stream with purchases
        df_purchases = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stream-purchases") \
            .option("failOnDataLoss", "false") \
            .load()

        df_clicks_watermark = df_parsed.withWatermark("event_ts", "10 seconds")

        query_stream_stream_join = (
            df_purchases
            .select(from_json(col("value").cast("string"),
                              kafka_message_schema)
                    # required alias for further selecting
                    .alias("json_data"),
                    "offset",
                    col("timestamp").alias("processing_ts"),
                    )
            .select("json_data.event_id", col("json_data.user_id").alias("purchase_user_id"),
                    col("json_data.value").alias("purchase_value"),
                    col("json_data.event_time").alias("purchase_event_ts"))
            .join(
                # watermarking is a must for stream-stream joins for right stream, left is optional
                df_clicks_watermark,
                expr("""
                purchase_user_id = user_id and
                purchase_event_ts between event_ts and event_ts + interval '10 seconds'
                """),
                "left"
            )
            .select("user_id", "purchase_event_ts", "purchase_value", col("event_ts").alias("click_event_ts"),
                    "processing_ts", "processing_date")
            # No watermark needed as there is already watermark after the join
            .dropDuplicates(["user_id", "purchase_event_ts", "purchase_value"])
            .writeStream
            .queryName("stream-stream join")
            .trigger(processingTime="5 seconds")
            # Output result to console
            .format("console")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir + "/stream_stream_join")
            .option("truncate", False)
            .option("numRows", 1000)
            .option("maxOffsetPerTrigger", 100)
            .start()
        )

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.info(f"Error: {e}")
