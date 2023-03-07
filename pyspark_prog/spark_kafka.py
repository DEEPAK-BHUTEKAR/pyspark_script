from pyspark.sql import *
from pyspark.sql.functions import *
from kafka import KafkaConsumer
import os
from kafka import KafkaProducer
from time import sleep
from json import dumps


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf8'))

data="D:/BigDataSoftware/livedata/"
filelist=os.listdir(data)

for file in filelist:
    with open(data+str(file),errors="ignore") as f:
        line=f.read()
        print(line)
        producer.send("indaus",line)
        sleep(5)

#kafka sent missions records per second, in order to analyse that we used sleep 5 ,
#it means every 5 sec data is generated and send to kafka brokers
