'''
App check higher heart rate activity
% producer.py M
› maritime application › & maritimeSpark.py>
% sparkApp2.py
E cons4. log
run with kafka:~/.local/bin/spark-submit
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 <file name`
'''

# read text file


# This file implements our health application. The file streams in heart rate data from a kafka topic and
# analyzes this data to determine if the person's heart rate is in a dangerous range (determined by using
# a threshold), thus determining in real-time if the person is at risk and needs medical attention. The result
# of this analysis is one of two values : AT RISK or SAFE, delineating whether the person's heart rate is in a
# dangerous range or not. This result is stored to another kafka topic.

# Note, this program assumes that the heart rate dataset has already been ingested to the input topic

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaProducer
# import json

# Some basic spark set up
spark = SparkSession.builder.appName("health app").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Taking the 2 topic names as command line arguments
parser = argparse.ArgumentParser(
    description="The names of the topics to input data from and output data to")
parser.add_argument("--input", metavar="", required=True,
                    help="The topic to input heart rate data from")
parser.add_argument("--output", metavar="", required=True,
                    help="The topic to output results to")
args = parser.parse_args()

input_topic = args.input
output_topic = args.output

# Heart rates greater than this value are dangerous for the person. This is just a test value. It can be replaced
# with the correct value upon consultation with the experts in the fields
threshold = 100


# We use epsilon for the floating point comparison when computing the result
EPSILON = 1e-10

# Reading the heart rate data into a streaming dataframe

df = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("failOnDataLoss", False)\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", input_topic)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

# heartRateSchema = StructType([ \
#     StructField("bpm", StringType()), \
#     StructField("time", TimestampType())])

# runningCount = df.groupBy().count()
# print('Running Count: {}'.format(int(str(runningCount))))
# This part checks whether the max heart rate is greater than the threshold, then sends the appropriate message as the result

class RowPrinter:

    # This part is just left as the default
    def open(self, partition_id, epoch_id):
        return True

    # This is where we check if the max heart rate is greater than the threshold or not, and send the appropriate message
    # to a kafka topic

    def process(self, row):
        import json
        global threshold
        global output_topic
        global EPSILON

        message = ""
        row = row.value.split(",")

        if (float(row[0]) - threshold) > EPSILON:
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            # json.dumps({"demo": "message"})
            # bytes(json.dumps(v, default=str).encode('utf-8')
            # json.dumps({"message": "AT RISK", "ts":row[1],"bpm":row[0]})
            # message = "AT RISK,{},{}".format(row[0],row[1])
            messageByte = bytes(json.dumps({"message": "AT RISK", "ts":row[1],"bpm":row[0]}), 'utf-8')
            producer.send(output_topic, messageByte)

        else:
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            # message = "SAFE,{},{}".format(row[0],row[1])
            messageByte = bytes(json.dumps({"message": "SAFE", "ts":row[1],"bpm":row[0]}), 'utf-8')
            # messageByte = bytes(message, 'utf-8')
            producer.send(output_topic, messageByte)
            # This part is also left as the default

    def close(self, error):
        print("Closed with error: %s" % str(error))


# Sending the results to the kafka topic. This basically executes all the processing of the above class

df.writeStream.outputMode("append").foreach(
    RowPrinter()).option("truncate", False).start().awaitTermination()

# df.writeStream.format("console").outputMode("complete").option("checkpointLocation", "/mnt/c/users/user/downloads/MLSpark/checkpoint/").start()\
#       .awaitTermination()
