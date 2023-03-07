from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").enableHiveSupport().getOrCreate()

#local[2] -->means in ur local s/m if u have 2 core utilize 2 core to process
#local[*] -->means in ur local s/m how many cores u have utilize all core to process
 #if u want to store data in hive enable hive using -- enableHiveSupport()

#data="s3://ph470/Jsonfile/zips.json"
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\zips.json"
df=spark.read.format("json").option("MODE","DROPMALFORMED").load(data)
df.printSchema()
df.show()

#data cleansing
res=df.withColumn("longitude",col("loc")[0]).withColumn("latitude",col("loc")[1])\
    .withColumnRenamed("_id","id").withColumn("id",col("id").cast(IntegerType()))\
    .withColumn("longitude",col("longitude").cast(FloatType()))\
    .withColumn("latitude",col("longitude").cast(FloatType()))

res.printSchema() #display columns and its datatypes in nice tree format
res.show(10)

#spark code write data into hive --table name is < zipjasontab >
#df.write.mode("append").format("hive").saveAsTable("zipjasontab")

#spark code write data into hive --external table name is < zipjasontab >
#df.write.format("hive").option("path","/hdfspath/hive/external").saveAsTable("zipjasonext")

