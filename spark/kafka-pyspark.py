from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaPySparkConsumer") \
    .getOrCreate()

# Define Kafka configurations
kafka_bootstrap_servers = "kafka:9092"  # Change based on your service name and port 
kafka_topic = "fullfillment.TEST_DB.test_table"  # Replace with your Kafka topic

# Define schema for the Kafka message value (assuming JSON messages)
message_schema = StructType([
    StructField("id", StringType(), True),
    StructField("value", StringType(), True)
])

# Read messages from Kafka topic
kafka_messages = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Select the key and value fields from the Kafka message and cast them to string
kafka_values = kafka_messages.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parse the JSON values (if your messages are in JSON format)
parsed_messages = kafka_values.withColumn("jsonData", from_json(col("value"), message_schema))

# Output the parsed data to the console (writeStream)
query = parsed_messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination(60)