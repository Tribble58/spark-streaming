import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, to_date, sum, count, window

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

output_dir = "/data/spark_output"

if __name__ == '__main__':
    try:
        spark = (SparkSession
                 .builder
                 .appName("tutorial")
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")

        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stream-input") \
            .option("failOnDataLoss", "false") \
            .load()

        # Define DDL
        kafka_message_schema = "event_id string, event_time timestamp, user_id int, action string, value int"

        query_parquet = (df
                         .select(from_json(col("value").cast("string"),
                                           kafka_message_schema)
                                 # required alias for further selecting
                                 .alias("json_data"),
                                 "offset",
                                 col("timestamp").alias("processing_ts"),
                                 )
                         .select("json_data.event_id", "json_data.user_id", "json_data.action", "json_data.value",
                                 col("json_data.event_time").alias("event_ts"), "offset",
                                 to_date(col("processing_ts")).alias("processing_date"))
                         .filter(col("action") == "click")
                         .writeStream
                         .trigger(processingTime="5 seconds")
                         # Output result to parquet
                         .format("parquet")
                         .option("path", output_dir)
                         .partitionBy("processing_date")
                         .outputMode("append")
                         .start()
                         )

        query_kafka = (df
                       .select(from_json(col("value").cast("string"),
                                         kafka_message_schema)
                               # required alias for further selecting
                               .alias("json_data"),
                               "offset",
                               col("timestamp").alias("processing_ts"),
                               )
                       .select("json_data.event_id", "json_data.user_id", "json_data.action", "json_data.value",
                               col("json_data.event_time").alias("event_ts"), "offset",
                               to_date(col("processing_ts")).alias("processing_date"))
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
                       # Output result to console
                       .format("kafka")
                       .option("kafka.bootstrap.servers", "kafka:9092")
                       .option("topic", "stream-output")
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

        query_console_watermark = (
            df
            .select(from_json(col("value").cast("string"),
                              kafka_message_schema)
                    # required alias for further selecting
                    .alias("json_data"),
                    "offset",
                    col("timestamp").alias("processing_ts"),
                    )
            .select("json_data.event_id", "json_data.user_id", "json_data.action", "json_data.value",
                    col("json_data.event_time").alias("event_ts"), "offset",
                    to_date(col("processing_ts")).alias("processing_date"))
            .filter(col("action") == "click")
            .select("user_id", "action", "value", "event_ts")
            .withWatermark("event_ts", "10 seconds")
            .groupBy(window("event_ts", "10 seconds", "5 seconds"))
            .agg(
                count("user_id").alias("count_in_window"),
                sum("value").alias("sum_in_window")
            )
            .writeStream
            # Output result to console
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .start()
        )

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.info(f"Error: {e}")
