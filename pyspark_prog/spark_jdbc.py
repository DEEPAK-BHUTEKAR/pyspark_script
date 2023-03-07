from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configparser import ConfigParser

# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

#data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\donations.csv"
#host="jdbc:mysql://sql-database.c9dlqkdtijxm.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
#user="myuser"
#pwd="mypassword"


cred="D:\\Data_Engg_Notes\\spark\\config.txt"
conf = ConfigParser()
conf.read(cred)
#read mysql_DB credentials from config file
host = conf.get("mycred","myhost")
uname = conf.get("mycred","myuser")
pwd = conf.get("mycred","mypass")
driver =conf.get("mycred","mydriver")

#read postgresql_DB credentials from config file
pshost = conf.get("pscred","pshost")
psuname = conf.get("pscred","puser")
pspwd = conf.get("pscred","pdpwd")
psdriver =conf.get("pscred","psdriver")


#qry="(select * from empres where totsal>2500)tt"

#RDBMS JDBC driver name
#---MySQL  -->	"com.mysql.jdbc.Driver"
#---ORACLE -->	"oracle.jdbc.driver.OracleDriver"
#--mssql -->""
#--postgresql --> "org.postgresql.Driver"

#read data from Mysql emp table
#df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password",pwd).option("dbtable","emp")\
# .option("driver",driver).load()

#importing/reading multiple table
#tabs=["dept","empclean","asl"]

#importing / reading all tables from mysqldb
qry="(select table_name from information_schema.tables where table_schema = 'mysqldb') tmp"
df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password",pwd)\
   .option("dbtable",qry).option("driver",driver).load()

tabs=[x[0] for x in df.collect()]
for x in tabs:
    print("importing table: " + x)
    df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password", pwd)\
        .option("dbtable", x ).option("driver", driver).load()
    df.show()
#writing data to postgresql
    df.na.fill(0).write.mode("append").format("jdbc").option("url",pshost).option("user",psuname).option("password",pspwd)\
             .option("dbtable",x + "1").option("driver",psdriver).save()





