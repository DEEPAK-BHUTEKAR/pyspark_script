from pyspark.sql import *
from pyspark.sql.functions import *

# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

