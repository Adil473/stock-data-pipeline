from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as _sum, max as _max, min as _min
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# Initialize Spark Session with Kafka and Postgres JDBC JAR packages
spark = SparkSession.builder \
    .appName("StockDataStreamingConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define JSON Schema matching the Producer payload
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("trade_timestamp", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True)
])

# 1. Read Stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stock-quotes") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON message values
parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Database connection configuration
JDBC_URL = "jdbc:postgresql://postgres:5432/stock_db"
JDBC_PROPERTIES = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# 2. Sink Function for Raw Data
def write_raw_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write \
            .mode("append") \
            .jdbc(JDBC_URL, table="raw_stock_data", properties=JDBC_PROPERTIES)

raw_query = parsed_stream.writeStream \
    .foreachBatch(write_raw_to_postgres) \
    .outputMode("append") \
    .start()

# 3. Streaming Transformation (Aggregations over 10-minute tumbling windows)
transformed_stream = parsed_stream \
    .withWatermark("trade_timestamp", "10 minutes") \
    .groupBy(
        col("ticker"),
        window(col("trade_timestamp"), "10 minutes")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        _sum("volume").alias("total_volume"),
        _max("price").alias("high_price"),
        _min("price").alias("low_price")
    ) \
    .select(
        col("ticker"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price"),
        col("total_volume"),
        col("high_price"),
        col("low_price")
    )

# 4. Sink Function for Transformed Data
def write_transformed_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write \
            .mode("append") \
            .jdbc(JDBC_URL, table="transformed_stock_data", properties=JDBC_PROPERTIES)

transformed_query = transformed_stream.writeStream \
    .foreachBatch(write_transformed_to_postgres) \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()