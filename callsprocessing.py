from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import os


# Create a Spark session
spark = SparkSession.builder.appName("KafkaToCSV").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

# Read data from Kafka topic with failOnDataLoss set to false
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "calls") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert the binary value of the Kafka message to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split the data into rows based on the semicolon delimiter
split_data = kafka_df.select(split(kafka_df["value"], ";").alias("data"))

# Create a DataFrame with columns "Call_Number"
df = split_data.select(
    col("data")[0].cast("int").alias("Total_day_calls")  # Assuming numbers are integers
)

# Calculate the remaining calls
df = df.withColumn("Remaining_Calls", 1000 - df["Total_day_calls"])

# Create the directory if it doesn't exist
csv_directory = "./remaining_calls"
os.makedirs(csv_directory, exist_ok=True)

# Specify the CSV path
csv_path = f"{csv_directory}/remainingcalls.csv"

checkpoint_directory = "./remaining_calls"
os.makedirs(checkpoint_directory,exist_ok=True)

checkpoint = f"{checkpoint_directory}"

query = df.writeStream \
    .format("csv") \
    .option("path", csv_path) \
    .option("checkpointLocation", checkpoint) \
    .option("header", "true") \
    .start()

# Wait for the streaming query to terminate (not required for batch processing)
query.awaitTermination()

# Stop the Spark session when you're done
spark.stop()
