from pyspark.sql import *
from pyspark.sql.functions import *
from operator import add
from functools import reduce

# creating spark session object

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\tax.txt"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.show()
#transpose columns: rows values convert to columns
#anywhere if u have nulls any columns replace with 0 use ... na.fill(0).. its replace all neumerical values
#.na.fill("Missing").na.fill(0) ... if any string column values contains null replace with "missing"
#if any number in any columns use na.fill(0)
#i want to replace nulls in specific column use ....na.fill(0,["ap","del"])
#.withColumn("totaltax", col("ap")+col("del")+col("tg")) don't hard code like this

ndf = df.groupBy("name").pivot("state").sum("amount").na.fill("unknown").na.fill(0)
#except name column take all columns next sum all these columns on fly
res=ndf.withColumn("totaltax", reduce(add,[ col(i) for i in ndf.columns if i not in "name"]))

res.show()

