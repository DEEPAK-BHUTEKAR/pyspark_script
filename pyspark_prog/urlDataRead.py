from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

#get data from url --https://apps.deadiversion.usdoj.gov/pubdisp/pub_disposal_sites.txt
url="https://apps.deadiversion.usdoj.gov/pubdisp/pub_disposal_sites.txt"
data=pd.read_csv(url,"|")

#NAME|ADDL CO INFO|ADDRESS 1|ADDRESS 2|CITY|STATE|ZIP|LATITUDE|LONGITUDE
#userSchema = StructType().add("name", "string").add("ADDLCOINFO", "string").add("ADDRESS1", "string").add("ADDRESS2", "string")\
#    .add("city","string").add("state","string").add("zip","integer").add("LATITUTE","double").add("longitude","double")
schema = StructType([StructField("name", StringType(), True)\
                   ,StructField("ADDLCOINFO", StringType(), True)\
                   ,StructField("ADDRESS1", StringType(), True)\
                     ,StructField("ADDRESS2", StringType(), True)
                     ,StructField("CITY", StringType(), True)
                     ,StructField("STATE", StringType(), True)
                     ,StructField("ZIP", IntegerType(), True)
                     ,StructField("LATITUDE", DoubleType(), True)
                     ,StructField("LONGITUDE", DoubleType(), True)])


#now pandas df convert to spark dataframe
df=spark.createDataFrame(data,schema=schema)
df.show()
