from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
# creating spark session object
spark = SparkSession.builder.master("local[*]").config("spark.driver.allowMultipleContexts", "true")\
        .appName("test").getOrCreate()

spark.conf.set("spark.driver.allowMultipleContexts", "true")
ssc=StreamingContext(spark.sparkContext, 10)
spark.sparkContext.setLogLevel("ERROR")
sqlurl="jdbc:mysql://mysqldb.cbaszrfvzjbi.ap-south-1.rds.amazonaws.com:3306/mysqldb"
driver="com.mysql.jdbc.Driver"

'''#spark internally uses different contexts to create different APIs
sparkContext -- to create rdd api
sqlContext -- to create DataFrame api
sparkSession -- to create dataset api
sparkStreamingContext ..it only an entry point to streaming data which generate Dstream api
    ..ssc ..create Dstream api(ssc convert live/stream data into spark understandable format )
  it internally uses sparkStreaming lib which process data in batch format with some delay(5/10ms) sent data to memory where it get process
   as spark process data with some delay hence it called near streaming engine/micro batch processing engine  


'''



# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

...

# DataFrame operations inside your streaming program

#words = ... # DStream of strings
#some server generate live data from port 1234; read data from  server
#socketTextStream is used to get data from console/terminal from something host/port no (hello world program)
host="ec2-35-154-53-93.ap-south-1.compute.amazonaws.com"
lines=ssc.socketTextStream(host,1234)
#lines.pprint()

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda x:x.split(","))
        df = rowRdd.toDF(["name","age","city"])
        df.show()
        hyd=df.where(col("city")=="hyd")
        pune = df.where(col("city") == "pune")

        hyd.write.mode("append").format("jdbc").option("url", sqlurl).option("user", "myuser")\
            .option("password", "mypassword").option("dbtable", "hydrecords").option("driver", driver).save()
        pune.write.mode("append").format("jdbc").option("url", sqlurl).option("user", "myuser") \
            .option("password", "mypassword").option("dbtable", "punerecords").option("driver", driver).save()

        # Creates a temporary view using the DataFrame
        #wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        #wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
        #wordCountsDataFrame.show()
    except:
        pass

lines.foreachRDD(process)

ssc.start()  #start computation
ssc.awaitTermination() # Wait for the computation to terminate


#run netcat server whcih is a small utility available in all linux ...
#to generate dummy data from port 1234... nc -lk 1234


#connectException: connection timeout error
#if u get above error add 1234 port and allow 0.0.0.0 access

