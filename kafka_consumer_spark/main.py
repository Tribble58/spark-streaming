import logging

from pyspark.sql import SparkSession

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
            .option("maxOffsetsPerTrigger", 10) \
            .option("subscribe", "stream-input") \
            .load()

        query = (df
                 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset",
                             "timestamp")
                 .writeStream
                 .trigger(processingTime="5 seconds")
                 .format("console")
                 .outputMode("append")
                 .start()
                 )
        query.awaitTermination()

    except Exception as e:
        logger.info(f"Error: {e}")
