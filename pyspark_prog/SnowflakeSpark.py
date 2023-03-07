from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("test")\
    .config("spark.jars","D:\BigDataSoftware\Spark_SoftWares\spark-3.1.2-bin-hadoop3.2\jars\spark-snowflake_2.12-2.11.0-spark_3.1")\
    .getOrCreate()

sc=spark.sparkContext
#hide the unnecessary error-log
sc.setLogLevel("ERROR")
# call the SnowflakeConnectorUtils
sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())


# You might need to set these
#sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIASS2AZ5BBR7PTM7HY")
#sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "X1q21jG0b3IxhsU6o5NEC8taHsZHnwCLH3BjROD7")

"""
#read from snowflake customer table -->"SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."CUSTOMER"
# Set options below
sfOptions = {
  "sfURL" : "fbqwibi-mq90187.snowflakecomputing.com",
  "sfUser" : "ph470",
  "sfPassword" : "Snowflake19",
  "sfDatabase" : "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema" : "TPCH_SF10",
  "sfWarehouse" : "SMALLWH"
}


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from customer limit 10")\
  .option("autopushdown","off").load()

df.show()
"""

#write into snowflake table FROM LOCAL COMPUTER/SYSTEM/S3
sfOptions = {
  "sfURL" : "fbqwibi-mq90187.snowflakecomputing.com",
  "sfUser" : "ph470",
  "sfPassword" : "Snowflake19",
  "sfDatabase" : "SFDB",
  "sfSchema" : "SPARK_SCHEMA",
  "sfWarehouse" : "SMALLWH"
}

#LOAD DATA FROM LOCAL SYSTEM
data="D:\Data_Engg_Notes\spark\Spark_dataset\\us500clean.csv"
df=spark.read.format("csv").option("header","true").option("sep",",")\
    .option("inferSchema","true").load(data)
df.show(10)

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME)\
  .options(**sfOptions) \
  .option("dbtable","us500tab")\
  .option("header","true").save()

