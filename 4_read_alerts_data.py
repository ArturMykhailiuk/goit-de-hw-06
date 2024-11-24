from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from configs import kafka_config
import os


# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())


# Читання потоку даних із Kafka (maxOffsetsPerTrigger - будемо читати 300 записів за 1 тригер.)
output_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "artur_home_building_sensors_out") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "300") \
    .option('failOnDataLoss','false')\
    .load()\
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
    .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))
    

# Перетворення значення з байтів у рядок і розбір JSON
json_schema = StructType([
    StructField('window', StructType([
        StructField('start', TimestampType(), True),
        StructField('end', TimestampType(), True)
    ]), True),
    StructField('avg_t', DoubleType(), True),
    StructField('avg_h', DoubleType(), True),
    StructField('code', StringType(), True),
    StructField('message', StringType(), True),
    StructField('timestamp', TimestampType(), True)
])

output_df = output_df.selectExpr(
    "CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value", from_json(col("value_deserialized"), json_schema)) \
    .select("key", "value.*")


# Виведення отриманих даних на екран
displaying_output_df = output_df.writeStream \
    .trigger(processingTime='10 seconds') \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
