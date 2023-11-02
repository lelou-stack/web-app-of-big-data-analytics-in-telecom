from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Create a Spark session
spark = SparkSession.builder.appName("KafkaToMySQL").config(
    "spark.driver.extraClassPath",
    "C:/Users/hhafs/.m2/repository/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar"
).config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
).getOrCreate()

# Read data from Kafka topic
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "chargedata") \
    .load()

# Convert the binary value of the Kafka message to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split the data into rows based on the semicolon delimiter
split_data = kafka_df.select(split(kafka_df["value"], ";").alias("data"))

# Create a DataFrame with columns "Total day charge"
df = split_data.select(
    col("data")[0].alias("Total_day_charge").cast("double")
)

# Define the JDBC URL and properties for your MySQL database
mysql_url = "jdbc:mysql://localhost:3306/pfa"
mysql_properties = {
    "user": "root",
    "password": "******",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Define the function to write the average to MySQL
def write_to_mysql(batch_df, batch_id):
    # Select the average value and create a DataFrame
    average_df = batch_df.selectExpr("avg(Total_day_charge) as avg_value")
    
    # Write the average to the "average" table in MySQL
    average_df.write.jdbc(url=mysql_url, table="average", mode="overwrite", properties=mysql_properties)

query = df.groupBy().avg("Total_day_charge").writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "path/to/checkpoint") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
