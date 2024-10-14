from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    num_data = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    result = num_data.sum()
    print(f"Sum of numbers is: {result}")
    spark.stop()