
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# import necessary jars
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/praveen/Downloads/spark-sql-kafka-0-10_2.11-2.4.0.jar,/home/praveen/Downloads/mysql-connector-java-8.0.14.jar,/home/praveen/Downloads/spark-streaming-kafka-0-10-assembly_2.11-2.4.0.jar pyspark-shell'

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("ReadFromKafkaStoreToMysql") \
    .getOrCreate()

# readStream for reading data from kafka topic test
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9094") \
  .option("subscribe", "test") \
  .load()

# Cast Value of dataframe in string and aliasing it as json
data =df.selectExpr("CAST(value AS STRING) as json")

# Create schema for streaming json objects
new_data = data.withColumn("id",F.get_json_object("json",'$.id'))\
    .withColumn("Product",F.get_json_object("json",'$.Product'))  \
    .withColumn("Price",F.get_json_object("json",'$.Price'))  \
    .withColumn("Payment_Type",F.get_json_object("json","$.Payment_Type"))  \
    .withColumn("Name",F.get_json_object("json","$.Name")).drop("json","$.json")

# Function to write eachbatch of kafka to mysql
def writeToSQL(df, epochId):
    df.write.format("jdbc")\
         .option("url","jdbc:mysql://localhost:3306/praveen")\
     .option("driver","com.mysql.cj.jdbc.Driver")\
     .option("dbtable","product")\
     .option("user","root")\
     .option("password","root")\
         .mode("append")\
     .save()

# Write data in mysql using foreachbatch and trigger in each 10 seconds, in foreachbatch call the method writeToSQL
# this method writeToSQL will write each batch data into mysql
query = new_data.writeStream.foreachBatch(writeToSQL).outputMode("append").trigger(processingTime='10 seconds').start().awaitTermination()


#query.awaitTermination()