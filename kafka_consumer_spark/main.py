import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    try:
        spark = (SparkSession
                 .builder
                 .appName("tutorial")
                 .getOrCreate())

        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stream-input") \
            .option("failOnDataLoss", "false") \
            .load()

        # Define DDL
        kafka_message_schema = "event_id string, event_time timestamp, user_id int, action string, value int"

        query = (df
                 .select(from_json(col("value").cast("string"),
                                   kafka_message_schema)
                         # required alias for further selecting
                         .alias("json_data"),
                         "offset",
                         col("timestamp").alias("processing_ts"),
                         )
                 .select("json_data.event_id", "json_data.user_id", "json_data.action", "json_data.value",
                         col("json_data.event_time").alias("event_ts"), "offset", "processing_ts")
                 .filter(col("action") == "click")
                 .writeStream
                 .trigger(processingTime="5 seconds")
                 # Output result to console
                 .format("console")
                 .outputMode("append")
                 .start()
                 )
        query.awaitTermination()

    except Exception as e:
        logger.info(f"Error: {e}")
