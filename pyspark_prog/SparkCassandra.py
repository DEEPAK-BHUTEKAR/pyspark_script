import sys
import os
from pyspark.sql import *
from pyspark.sql.functions import *
import findspark
findspark.init()
os.environ["JAVA_HOME"]="/usr/lib/jvm/java"
os.environ["SPARK_HOME"]="/usr/lib/spark"
# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
adf=spark.read.format("org.apache.spark.sql.cassandra").option("table","asl")\
    .option("keyspace","cassdb").load()
#adf.show()
edf=spark.read.format("org.apache.spark.sql.cassandra").option("table","emp")\
    .option("keyspace","cassdb").load()
#edf.show()
#bydefault its inner join --> join=adf.join(edf,edf.first_name==adf.name) also work
#if  both df has same column name at that time use --> join=adf.join(edf,"joinType")
#display result of left outer join
lfjoin=adf.join(edf,edf.first_name==adf.name,"left").drop("first_name").na.fill("no data").na.fill(0)
#for string type null values enter no data while for integer type null value enter 0
lfjoin.show()
#display result of right outer join
rsjoin=adf.join(edf,edf.first_name==adf.name,"right").drop("first_name").na.fill("no data").na.fill(0)
rsjoin.show()

#write data into cassandra aslempjoin table
lfjoin.write.mode("append").format("org.apache.spark.sql.cassandra").option("table","aslempjoin").option("keyspace","cassdb").save()

#//while loading data into cassandra u get one error--> Caused by: java.lang.NoClassDefFoundError: com/twitter/jsr166e/LongAdder
#wget https://repo1.maven.org/maven2/com/twitter/jsr166e/1.1.0/jsr166e-1.1.0.jar

