from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when

# Create a Spark session
spark = SparkSession.builder.appName("KafkaToMySQL").config("spark.driver.extraClassPath","C:/Users/hhafs/.m2/repository/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

# Read data from Kafka topic with failOnDataLoss set to false
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "chargedata") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert the binary value of the Kafka message to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split the data into rows based on the semicolon delimiter
split_data = kafka_df.select(split(kafka_df["value"], ";").alias("data"))

# Create a DataFrame with columns "Total_day_charge"
df = split_data.select(
    col("data")[0].cast("double").alias("Total_day_charge")
)

avg = 30.515905511811063
max_avg = avg + (avg * 0.2)
min_avg = avg - (avg * 0.2)

# Create three columns for charge categories
df = df.withColumn("Total_day_charge", when(df["Total_day_charge"] > max_avg, "Above avg")
    .when((df["Total_day_charge"] >= min_avg) & (df["Total_day_charge"] <= max_avg), "in avg")
    .otherwise("Under avg"))


# Group by "Total_day_charge" and count the number of members in each category
result_df = df.groupBy("Total_day_charge").count().orderBy("Total_day_charge")

# Define the JDBC URL and properties for your MySQL database
mysql_url = "jdbc:mysql://localhost:3306/test"
mysql_properties = {
    "user": "root",  # Replace with your MySQL username
    "password": "******",  # Replace with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

def write_to_mysql(batch_df, batch_id):
    try:
        batch_df.write.jdbc(url=mysql_url, table="charge", mode="overwrite", properties=mysql_properties)
        print("Data successfully written to MySQL")
    except Exception as e:
        print("Error writing to MySQL:", str(e))

# Write the results to the MySQL table using foreachBatch
query = result_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "path/to/checkpoint") \
    .queryName("MySQLWriteQuery") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
