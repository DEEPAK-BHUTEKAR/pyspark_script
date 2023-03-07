from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

'''

#Q.read the data but skip the header line
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\donations.csv"
df=spark.read.format("csv").option("header","True").load(data)

#option("header","True") -->first line of data consider as header
df.createOrReplaceTempView('donationtab')
#write query to display total ammount recived by each individual/person
res=spark.sql('select name , sum(amount) as sum_sal from donationtab group by name order by name ')
res.show()



#Q.clean unstructure data skip first line --> skip header line

data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\donations_dup.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
rdata=rdd.filter(lambda x: x!=skip)
df=spark.read.csv(rdata, header=True, inferSchema=True)
df.show(5)  #show top  5 lines
df.printSchema()
#printSchema will print column_name and its datatypes in tree format
'''

#Q.filter/process unstructure data where we cant use dataFrame we hv to use rdd
data ="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\emailsmay4.txt"
erdd=sc.textFile(data)
res=erdd.filter(lambda x: "@" in x).map(lambda x: x.split(" ")).map(lambda x: (x[0],x[-1]))

for i in res.collect():
    print(i)
