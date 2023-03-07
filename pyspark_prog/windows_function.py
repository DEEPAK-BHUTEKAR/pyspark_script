from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import  *
from pyspark.sql.window import Window
# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)\
.withColumnRenamed("zip","sal").select("first_name","last_name","state","sal")
#df.printSchema()
#df.show()
'''

#there are 8 types of window function are available...
#5 ranking fun()--{ row_number(),rank(),denserank(),percert_rank(),ntile() } and analytical func()--{lead,lag,first,last}
#dense_rank() -- u'll get ranks in sequence order irrespctive of duplicate records
#rank() --u'll get rank sequence but for every duplicate records it will skip consecative number
#row_number() -- row_number is basically unique id column irrespective of records
#percent_rank()--
#ntile()--  function returns the relative rank of result rows within a window partition.

win=Window.partitionBy(col("state")).orderBy(col("sal").desc())
res=df.withColumn("rnk",rank().over(win)).withColumn("dsrnk",dense_rank().over(win))\
    .withColumn("rno",row_number().over(win)).withColumn("prank",percent_rank().over(win))\
    .withColumn("ntile",ntile(4).over(win))\
    .withColumn("lag",lag(col("sal")).over(win)).withColumn("lead",lead(col("sal")).over(win))\
    .withColumn("diff",col("sal")- col("lead"))
res.show()
'''
#analytical func()--{lead,lag,first,last}

win=Window.orderBy(col("sal").desc())
res1=df.withColumn("lag", lag(col("sal")).over(win)).withColumn("lead", lead(col("sal")).over(win)) \
    .withColumn("diff", col("sal") - col("lead")).withColumn("fst",first(col("sal")).over(win))\
    .withColumn("lst",last(col("sal")).over(win)) 
res1.show()