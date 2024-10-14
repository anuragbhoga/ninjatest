from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaPySparkConsumer") \
    .getOrCreate()

# Define Kafka configurations
kafka_bootstrap_servers = "kafka:9092"  # Change based on your service name and port
kafka_topic = "fullfillment.TEST_DB.test_table"  # Replace with your Kafka topic

# Define the schema for the Kafka message payload 
payload_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("source", StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True)
    ]), True),
    StructField("op", StringType(), True)
])

# Read messages from Kafka topic
kafka_messages = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Select the value field from the Kafka message and cast it to string
kafka_values = kafka_messages.selectExpr("CAST(value AS STRING)")

# Extract and parse the "payload" part from the JSON
parsed_payload = kafka_values.withColumn("payload", from_json(col("value"), payload_schema)).select("payload.*")

# Select relevant fields from the parsed payload for tabular display
tabular_data = parsed_payload.select(
    col("source.db").alias("Database"),
    col("source.table").alias("Table"),
    col("after.id").alias("ID"),
    col("after.name").alias("Name"),
    col("op").alias("Operation Type")
)

# Output the tabular data to the console
query = tabular_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Await termination of the query
query.awaitTermination(60)