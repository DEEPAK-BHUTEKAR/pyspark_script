from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
#filter/process unstructure data where we cant use dataFrame we hv to use rdd
data ="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\emailsmay4.txt"
erdd=sc.textFile(data)
res=erdd.filter(lambda x: "@" in x).map(lambda x: x.split(" ")).map(lambda x: (x[0],x[-1]))

for i in res.collect():
    print(i)