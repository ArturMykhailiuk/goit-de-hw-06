from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from configs import kafka_config
import os
import datetime


# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.streaming.backpressure.enabled", "true")
         .config("spark.streaming.kafka.maxRatePerPartition", "200")
         .getOrCreate())


# Читання потоку даних із Kafka (maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.)
input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "artur_home_building_sensors_in") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .option('failOnDataLoss','false')\
    .load()\
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
    .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))


## 2 ##
# Агрегація даних - середня температура та середня вологіс
json_schema = StructType([
    StructField('sensor_id', IntegerType(), True),
    StructField('timestamp', StringType(), True),
    StructField('temperature', DoubleType(), True),
    StructField('humidity', DoubleType(), True)
])

input_df = input_df.selectExpr("CAST(value AS STRING) AS value_deserialized") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("sensor_id", col("value_json.sensor_id")) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("temperature", col("value_json.temperature")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), "sensor_id") \
    .agg(avg("temperature").alias("avg_t"), avg("humidity").alias("avg_h"))\
    .drop("value_deserialized", "value_json")

# Виведення отриманих даних на екран
# displaying_input_df = input_df.writeStream \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start() \
#     .awaitTermination()


## 3 ##
# Зчитування параметрів алертів з файлу
alerts_conditions = spark.read.csv("alerts_conditions.csv", header=True)


## 4 ##
#Побудова визначення алертів
crossed_df = input_df.crossJoin(alerts_conditions)

alerts_df = crossed_df\
    .where("(avg_t > temperature_min AND avg_t < temperature_max) OR (avg_h> humidity_min AND avg_h < humidity_max)") \
    .withColumn("timestamp", lit(str(datetime.datetime.now()))) \
    .drop("id")
     
# displaying_alerts_df= alerts_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()\
#     .awaitTermination()


## 5 ##
#Запис даних у Kafka-топік
alerts_query = alerts_df.selectExpr(
    "CAST(sensor_id AS STRING) AS key",
    "to_json(struct(window, avg_t, avg_h, code, message, timestamp)) AS value") \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "artur_home_building_sensors_out") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-alerts-1") \
    .start()\
    .awaitTermination()