from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

#load asl file and convert it to rdd
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\asl.csv"
erdd=sc.textFile(data)
skip=erdd.first()

#performing transformations and actions on aslrdd
#if u want to apply the logic on particular column u have to split the column using map() first then apply the logic
#but split bydefault convert everything to string type
#res=aslrdd.map(lambda x:x.split(","))
#skip the header using logic < skip=aslrdd.first() >

#res=aslrdd.filter(lambda x:x != skip).map(lambda x:x.split(",")).filter(lambda x: int(x[1])>30)

res=erdd.filter(lambda x:x != skip).map(lambda x:x.split(",")).filter(lambda x: 'hyd' in x[2])

for i in res.collect():
    print(i)



