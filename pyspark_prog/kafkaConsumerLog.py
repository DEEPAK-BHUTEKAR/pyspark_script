from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaProducer
from kafka import KafkaConsumer
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "mar7") \
    .load()
#write the log data into mysql database
host="jdbc:mysql://sqldatabase.c9dlqkdtijxm.ap-south-1.rds.amazonaws.com :3306/mysqldb"
uname="myuser"
pwd="mypassword"
mydriver="com.mysql.jdbc.Driver"
#df.printSchema()
df1 = df.selectExpr("CAST(value AS STRING)")

reg = """^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\""""
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = df1.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF.writeStream.outputMode("append").format("console").start().awaitTermination()

db_target_properties = {"user":"myuser", "password":"mypassword"}
def foreach_batch_function(df, epoch_id):
    df.write\
        .jdbc(url='"jdbc:mysql://sqldatabase.c9dlqkdtijxm.ap-south-1.rds.amazonaws.com :3306/mysqldb"',  table="sparkkafka",  properties=db_target_properties)
    pass
qry=logsDF.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
qry.awaitTermination()

#logsDF.writeStream.outputMode("append").format("jdbc").option("url",host).option("user",uname).option("password",pwd).option("dbtable", "kafka_log").option("driver",mydriver).start().awaitTermination()


