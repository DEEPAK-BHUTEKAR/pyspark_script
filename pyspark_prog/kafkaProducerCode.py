from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaProducer
from kafka import KafkaConsumer
import re
from time import sleep
from json import dumps

spark=SparkSession.builder.master("local[*]").appName("test").getOrCreate()

df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "mar7").load()
data="C:\\bigdata\\logs\\access_log_20230307-193508.log"
#read data from log file and sent it to kafka broker
producer=KafkaProducer(bootstrap_servers= ['localhost:9092'],
                        value_serializer=lambda x:
                       dumps(x).encode('utf8-'))

with open (data,mode="r") as f:
    for line in f:
        print(line)
        producer.send("mar7",line)
        sleep(5)

#df.printSchema()

