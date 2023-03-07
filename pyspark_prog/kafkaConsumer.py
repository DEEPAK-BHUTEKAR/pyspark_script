from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "mar7") \
  .load()

#df.printSchema()
df1 = df.selectExpr("CAST(value AS STRING)")


def read_nested_json(df):
  column_list = []
  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      df = df.withColumn(column_name, explode(column_name))
      column_list.append(column_name)
    elif isinstance(df.schema[column_name].dataType, StructType):
      for field in df.schema[column_name].dataType.fields:
        column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
    else:
      column_list.append(column_name)
  df = df.select(column_list)
  return df


def flatten(df):
  read_nested_json_flag = True
  while read_nested_json_flag:
    df = read_nested_json(df)
    read_nested_json_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True
  cols = [re.sub('[^a-zA-Z0-9_]', "", c.lower()) for c in df.columns]
  return df.toDF(*cols)

ndf = flatten(df1)
def foreach_batch_function(df, epoch_id):
  sfOptions = {
    "sfURL" : "fbqwibi-mq90187.snowflakecomputing.com",
    "sfUser" : "ph470",
    "sfPassword" : "Snowflake19",
    "sfDatabase" : "CONTROL_DB",
    "sfSchema" : "public",
    "sfWarehouse" : "COMPUTE_WH"
  }
  SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  df.write.format(SNOWFLAKE_SOURCE_NAME).options(*sfOptions).option("dbtable", "kaflive").mode("append").save()

  pass




ndf.writeStream.foreachBatch(foreach_batch_function).start()

#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()







