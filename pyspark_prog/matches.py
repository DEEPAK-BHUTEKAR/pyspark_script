from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
# creating spark session object

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\matches.csv"
#read data from mactch.csv
match_df=spark.read.csv(data,header=True,inferSchema=True)
# teams who played three consecutive matches
# player of series for each season - player who received maximum man_of_match award each season
# winner of each season
# entire row of stats for final of each season.
match_df=match_df.withColumn("date",to_date("date","yyyy-MM-dd"))
match_df.printSchema()
match_df.show()

df=match_df.groupBy(col("season")).agg(max("id").alias("finalmatchid")).sort("season").withColumnRenamed("season","season1")
df.show()
#season wise winner of ipl season
res=df.join(match_df,col("finalmatchid")==col("id"),"left")
print("season wise winner of ipl season")
res.select("season","winner").sort("season").show()

#
res2 = match_df.groupBy("season","player_of_match").agg(count("player_of_match").alias("count_playerofmatch"))\
    .sort("season").withColumnRenamed("season","season1")
res3=res2.groupBy("season1",).agg(max("count_playerofmatch").alias("max_pom")).sort("season1").withColumnRenamed("season1","season2")

#cond=[col("season1")==col("season2"),col("count_playerofmatch")==col("max_pom")]

fin=res3.join(res2,[col("season1")==col("season2"),col("count_playerofmatch")==col("max_pom")],"left")
print("season wise player of series")

fin.withColumnRenamed("season2","season").withColumnRenamed("player_of_match","playerofseries").sort("season")\
    .drop("count_playerofmatch","max_pom","season1").show()




match_df.createOrReplaceTempView("matchtab")
#res=spark.sql("SELECT * FROM ( SELECT DISTINCT ON (season) season, date,winner FROM matchtab  ORDER BY season, date ASC) s order by date desc ")
#res=spark.sql("select season,winner  from matches group by season,winner order by season, date desc;")

# Which team has won most number of matches in IPL?

spark.sql(
"""
select season,winner,count(winner) from matchtab
group by season,winner
order by count(winner) desc 
""").show()
#

#Which team won the match by biggest margin?

spark.sql(
"""
select id , winner , win_by_runs from matchtab
where win_by_runs != 0
order by win_by_runs desc
""").show(5)




