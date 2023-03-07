from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","True").load(data)

#count the no of employees per state arrange them in descending order
#res=df.groupBy(col("state")).agg(count("*").alias("cnt").orderBy(col("cnt").desc())

#res=df.withColumn("age",lit(18)).withColumn("web",lit("url"))
#--withColumn() -->add new column (if column not exist) or update existing column values (if column exists)
#lit(value) -- used to add something dummy values

"""
res=df.withColumn("full_name",concat_ws(" ",df.first_name,df.last_name))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))\
    .withColumn("phone2",regexp_replace(col("phone2"),"-","").cast(LongType()))\
    .drop("address","zip","web","company_name")\
    .withColumnRenamed("first_name","fname").withColumnRenamed("last_name","lname")
res.show()
res.printSchema()

"""
#concat_ws () -->concat column with seperator
#regex_replace(col_name,pattern,replace_str) -->replace column values base on pattern
#cast(DataType) -->convert /change column  datatype  for this import -->from pyspark.sql.types import *
#drop() -->delete / remove unwanted columns
#withColumnRenamed("old_name","new_name") -->replace column name


#--Q.print list of prople as per state who has got covid vaccine -->used collect_list() -->may
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt"),collect_list(df.email)).orderBy(col("cnt").desc())

#--Q.print list of cities per state (unique records) who has got fully vaccinated with covid -->used collect_set()
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt"),collect_set(df.city)).orderBy(col("cnt").desc())
#collect_set() -->give unique list of cities from state


#use of when statement used -->update records
#ndf=df.withColumn("state",when(col("state")=="NY" ,"Newyork").when(col("state")=="CA" ,"cali").otherwise (col("state")))

#Q.what is diff between regex_replace()-->(particular pattern only is replace) and when statement -->entire statement is remove
#--Q.update column if column has # -->***** otherwise  -->keep column as it is
#ndf=df.withColumn("address1",when(col("address").contains("#"),"*****")\
#    .otherwise(col("address"))).withColumn("address2",regexp_replace("address","#","@"))

#Q.find count of gmail acc u got use substring_index() have
ndf1=df.withColumn("username",substring_index(col("email"),"@",1))\
    .withColumn("emails",substring_index(col("email"),"@",-1))
#--display emails count in descending order
ndf=ndf1.groupBy(col("emails")).count().orderBy(col("count").desc())

#substring_index() -->use to locate str before or after the symbol/pattern
# --here -1 right hand side string and +1 give left hand side string


ndf.printSchema()
ndf.show(truncate=False)

