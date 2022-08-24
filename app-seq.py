import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, from_json, col, when, udf
from pyspark.sql.types import StringType, StructType, StructField
from kafka import KafkaProducer
import time
import uuid

# Some basic spark set up
spark = SparkSession.builder.appName("health app").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Taking the 1 topic name as command line arguments
parser = argparse.ArgumentParser(
    description="The names of the topics to input data from and output data to")
parser.add_argument("--input", metavar="", required=True,
                    help="The topic to input heart rate data from")
parser.add_argument("--output", metavar="", required=False,
                    help="The topic to output results to")
args = parser.parse_args()

output_topic = args.output
input_topic = args.input
max_occurrence = 5


heartRateSchema = StructType([StructField("ts", StringType(), True),
                              StructField("message", StringType(), True),
                              StructField("bpm", StringType(), True)])

df2 = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("failOnDataLoss", False)\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", input_topic)\
    .load()\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
    .select(from_json(col("value"), heartRateSchema).alias("data"))\
    .selectExpr("data.message AS status", "data.bpm AS bpm", "data.ts AS timestamp")

query = df2.writeStream.format("console").option("truncate", "false").start()
time.sleep(10)  # sleep 10 seconds
query.stop()

df2.printSchema()

#
# Group count of status occurences in the stream according to the specified interval
#
windowedCounts = df2.groupBy(df2.status, window(
    df2.timestamp, "2.50000 seconds", "0.50000 seconds")).count()

windowedCounts.printSchema()

def func(status, count):
    if status == None or count == None:
        return "N"
    if status == "AT RISK" and count >= max_occurrence:
        return "Y"
    return "N"


func_udf = udf(func, StringType())

# Set Alert to Y if status is "AT RISK" 5 consecutive times or repeatedly 
# over the couurse of 2.5 seconds. Given that every record is generated
# every 0.5 seconds
# We say 5 consecutive times to reflect the max_occurrence set in the code
# This could very well be a variable.
query3 = windowedCounts.withColumn("Alert", func_udf(
    windowedCounts['status'], windowedCounts['count']))

query1 = query3.writeStream \
    .outputMode("complete") \
    .option("truncate", "false") \
    .format("console") \
    .start()\
    .awaitTermination()
