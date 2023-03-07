from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\null_value.csv"
#data="/FileStore/tables/null_value.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","True").load(data)
df.printSchema()
df.show()

#DataFrame.fillna()-->df.fillna() or DataFrameNaFunctions.fill()--> df.fillna()
#In PySpark, df.fillna() or df.fillna() is used to replace NULL/None values on all or selected multiple
#DataFrame columns with either zero(0), empty string, space, or any constant literal values.

#PySpark Replace NULL/None Values with Zero (0)/any int value for all column with integer datatype
res=df.na.fill(0)
res.show()

#Q.PySpark Replace NULL/None Values with Zero (0) for particular column with integer datatype  i.e polpulation
#res=df.na.fill(value=0,subset=["population"])
#res.show()

#PySpark Replace Null/None Value with Empty String
res1=df.na.fill(" ").show()

#PySpark Replace Null/None Value with String like "not available"/"invalid"/NA/...etc
res2=df.na.fill("unavailable")
res2.show()

#applying na.fill() on multiple column with the help of dictionary
res3=df.na.fill({"type":"un_define","city":"Outsider","population":0})
res3.show()