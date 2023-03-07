from pyspark.sql import *
from pyspark.sql.functions import *

# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
#IN ORDER TO AVOID UNNECESSARY ERRORS; USE setLogLevel("ERROR")
spark.sparkContext.setLogLevel("ERROR")
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\bank-full.csv"
df=spark.read.format("csv").option("header","True").option("inferschema","True").option("sep",";").load(data)
#sep option used to specify delimiter
#in spark every field consider as string use inferSchema as True for making  datatype other than string
#inferSchema -- auto convert data to appropriate datatype means int if value is 454
#process programiing friendly
#res=df.where(col("age")>90)
#res=df.where((col("age")>90) & (col("marital")=="married"))
#select partial columns age and marital
#res=df.select(col("age"),col("marital")).where(col("age")>90)
#print records of married people above 90 yrs age
#res=df.groupBy(col("marital")).agg(sum(col("balance")).alias("bal_amt")).orderBy(col("bal_amt").desc())
#res=df.groupBy(col("marital")).count()
res=df.groupBy(col("marital")).agg(count("*").alias("count"),sum(col("balance")).alias("bal_amt")).orderBy(col("bal_amt").desc())

res.show()

res.printSchema()
'''

#process sql friendly
df.createOrReplaceTempView("emp_tab")
#df.createOrReplaceTempView("emp_tab") register this df as a table it allow usage of sql queries
#res=spark.sql("select age, marital from emp_tab where age>60 and marital != 'married' ")
#Q.as per marital status count their number and any sum of their balance
res=spark.sql("select marital,count(*) ppl_count,sum(balance) as sum_bal from emp_tab group by (marital)  ")

res.show()
res.printSchema()
'''