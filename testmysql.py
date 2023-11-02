from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split

# Create a Spark session
spark = SparkSession.builder.appName("KafkaToMySQLPercentage").config(
    "spark.driver.extraClassPath",
    "C:/Users/hhafs/.m2/repository/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar"
).config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
).getOrCreate()

# Define the JDBC URL and properties for your MySQL database
mysql_url = "jdbc:mysql://localhost:3306/pfa"
mysql_properties = {
    "user": "root",
    "password": "*******",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read the average from MySQL
average_df = spark.read.jdbc(url=mysql_url, table="average", properties=mysql_properties)

# Read data from Kafka topic
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option(
    "subscribe", "chargedata"
).load()

# Convert the binary value of the Kafka message to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split the data into rows based on the semicolon delimiter
split_data = kafka_df.select(split(kafka_df["value"], ";").alias("data"))

# Create a DataFrame with columns "Total day charge"
data_df = split_data.select(col("data")[0].alias("Total_day_charge").cast("double"))

# Calculate the percentage values
joined_df = data_df.crossJoin(average_df)
above = joined_df.filter(joined_df["Total_day_charge"] > (col("avg(Total_day_charge)") * 1.2))
below = joined_df.filter(joined_df["Total_day_charge"] < (col("avg(Total_day_charge)") * 0.8))
within = joined_df.filter((joined_df["Total_day_charge"] >= (col("avg(Total_day_charge)") * 0.8)) & (
        joined_df["Total_day_charge"] <= (col("avg(Total_day_charge)") * 1.2)))

# Count the above, below, and within values
above_count = above.groupBy().count()
below_count = below.groupBy().count()
within_count = within.groupBy().count()


# Define the function to write the percentage data to MySQL
def write_percentage_to_mysql(batch_df, batch_id):
    batch_df.write.jdbc(url=mysql_url, table="percentage_data", mode="overwrite", properties=mysql_properties)

# Write the percentage data to MySQL using foreachBatch
query = within.selectExpr(f"'{above_count}' AS above_count", f"'{below_count}' AS below_count",
                         f"'{within_count}' AS within_count").writeStream.outputMode("append").foreachBatch(
    write_percentage_to_mysql).option("checkpointLocation", "path/to/checkpoint_percentage").start()

# Wait for the streaming query to terminate
query.awaitTermination()

