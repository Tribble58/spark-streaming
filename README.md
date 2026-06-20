# Spark Streaming repository

## Description

The following project is dedicated to learning **_Spark Structured Streaming_**, especially:

- reading static files, streams of different formats (Kafka, parquet, socket, etc.) with various schemas
- defining those schemas in different ways (SQL, DSL)
- performing stateless transformations (select, filter, map)
- implementing stateful transformations (grouping, joining, aggregating)
- working with watermarks, time range joins for stream-stream connection
- writing streams to parquet, Kafka, console
- partitioning data for data speed access increase
- error handling, checkpoint locations managing

All services are packed and delivered as **Docker containers bundle**, follow the instructions below to run it on
your machine.

## Step-by-step algorithm

1. **reads csv** users static data from /data/static/csv directory on your machine with specific schema defined in
   _users_schema_
2. **reads** clicks and purchases **streams** that are produced by Python applications that generate messages and send
   them to Kafka topics (**_stream-clicks_** and _**stream-purchases**_). Some clicks are sent with delay, some purchases are
   sent duplicated;
3. **deserializes** Kafka messages to DataFrame columns, adds processing timestamp column;
4. **joins** clicks stream with static users data and write it to _**/data/output/parquet/clicks_enriched**_;
5. **adds watermark** on both streams;
6. **performs stream-stream join** based on watermarks and time range (purchase time between click time and click time + 1
   minute) and **outputs results to Kafka** attributed-purchases topic and console;
7. **calculates** count of purchases and total amount of purchase during 10 seconds window with 5 seconds slide and **writes
   result to parquet** _**/data/output/parquet/aggregated_purchases**_;
8. **removes duplicates** in clicks stream and **writes results in parquet** to  _**/data/output/parquet/deduplicated_clicks**_;

## Instruction

1. First things first, make sure you have Docker installed on your machine. If not, follow
   the [Docker installation guide](https://docs.docker.com/engine/install/);
2. Pull current repository to the desired directory on your machine;
3. Run docker compose file:

```docker
docker compose up
```

4. Enjoy! :)
