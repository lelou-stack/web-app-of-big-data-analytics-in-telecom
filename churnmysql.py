from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

# Create a Spark session
spark = SparkSession.builder.appName("KafkaChurnCount").config("spark.driver.extraClassPath", "C:/Users/hhafs/.m2/repository/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

# Read data from Kafka topic with failOnDataLoss set to false
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "churn") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert the binary value of the Kafka message to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Define the logic to count "true" and "false" values
count_df = kafka_df.select(when(col("value") == "True", 1).otherwise(0).alias("is_true"),
                          when(col("value") == "False", 1).otherwise(0).alias("is_false"))

# Calculate the counts
result_df = count_df.groupBy().sum("is_true", "is_false")

# Define the JDBC URL and properties for your MySQL database
mysql_url = "jdbc:mysql://localhost:3306/pfa"
mysql_properties = {
    "user": "root",  # Replace with your MySQL username
    "password": "*******",  # Replace with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

def write_to_mysql(batch_df, batch_id):
    try:
        batch_df.write.jdbc(url=mysql_url, table="churn_counts", mode="overwrite", properties=mysql_properties)
        print("Counts successfully written to MySQL")
    except Exception as e:
        print("Error writing to MySQL:", str(e))

# Write the counts to the MySQL table using foreachBatch
query = result_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "path/to/checkpoint") \
    .queryName("MySQLWriteQuery") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
