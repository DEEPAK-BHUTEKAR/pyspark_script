from pyspark.sql import *
from pyspark.sql.functions import *


# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

Access_key_ID="AKIASS2AZ5BBR7PTM7HY"
Secret_access_key="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"

# Enable hadoop s3a settings
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",Access_key_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",Secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

data="s3a://ph470/input/asl.csv"
df=spark.read.format('csv').option("header","true").option("inferSchema","true").load(data)
df.show()
