from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("gluejobdemo").getOrCreate()
sc=spark.sparkContext
#hide the unnecessary error-log
spark.sparkContext.setLogLevel("ERROR")

# call the SnowflakeConnectorUtils
#sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "SFDB"
    snowflake_schema = "SPARK_SCHEMA"
    snowflake_warehouse="SMALLWH"
    source_table_name = "BANKTAB"
    snowflake_options={
    "sfURL": "fbqwibi-mq90187.snowflakecomputing.com",
    "sfUser": "ph470",
    "sfPassword": "Snowflake19",
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": snowflake_warehouse
    }
    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable",snowflake_database+"."+snowflake_schema+"."+source_table_name )\
        .option("autopushdown", "off").load()

    df1=df.groupBy(col("marital")).agg(count("*"))
    df1.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "bank_count") \
        .option("header", "true").save()

main()

#write read data from snowflake and write data into s3

def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "SFDB"
    snowflake_schema = "SPARK_SCHEMA"
    snowflake_warehouse="SMALLWH"
    source_table_name = "BANKTAB"
    snowflake_options={
    "sfURL": "fbqwibi-mq90187.snowflakecomputing.com",
    "sfUser": "ph470",
    "sfPassword": "Snowflake19",
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": snowflake_warehouse
    }
    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("query","select marital ,count(*) as people_count from banktab group by marital order by people_count" )\
        .option("autopushdown", "off").load()

    df.coalesce(1).write.option("mode","overwrite").option("header","true").csv("s3://ph470/output/snow_op/")



main()