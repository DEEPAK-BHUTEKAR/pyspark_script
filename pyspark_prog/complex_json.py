from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data="D:/Data_Engg_Notes/spark/Spark_dataset/world_bank.json"
#--load the json file fro local system
df=spark.read.format("json").load(data)

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df;


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    cols = [re.sub('[^a-zA-Z0-9_]', "", c.lower()) for c in df.columns]
    return df.toDF(*cols);


ndf = flatten(df)
ndf.show()
ndf.printSchema()


"""

# --flatten the complex datatypes of world_bank.json file
df=df.withColumn("id_oid",col("_id.$oid")).drop("_id")\
    .withColumn("majorsector_percent",explode("majorsector_percent")).withColumn("majorsector_percent_name",col("majorsector_percent.name"))\
    .withColumn("majorsector_percent_percent",col("majorsector_percent.percent")).drop("majorsector_percent")\
    .withColumn("mjsector_namecode",explode("mjsector_namecode")).withColumn("mjsector_namecode_code",col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name",col("mjsector_namecode.name")).drop("mjsector_namecode")\
    .withColumn("mjtheme",explode("mjtheme"))\
    .withColumn("mjtheme_namecode",explode("mjtheme_namecode")).withColumn("mjtheme_namecode_code",col("mjtheme_namecode.code"))\
    .withColumn("mjtheme_namecode_name",col("mjtheme_namecode.name")).drop("mjtheme_namecode")\
    .withColumn("project_abstract_cdata",col("project_abstract.cdata")).drop("project_abstract")\
    .withColumn("projectdocs",explode("projectdocs")).withColumn("projectdocs_docdate",col("projectdocs.DocDate"))\
    .withColumn("projectdocs_DocType",col("projectdocs.DocType")).withColumn("projectdocs_DocTypeDesc",col("projectdocs.DocTypeDesc"))\
    .withColumn("projectdocs_DocURL",col("projectdocs.DocURL")).withColumn("projectdocs_EntityID",col("projectdocs.EntityID"))\
    .drop("projectdocs")\
    .withColumn("sector",explode("sector")).withColumn("sector_name",col("sector.name")).drop("sector")\
    .withColumn("sector1_name",col("sector1.name")).withColumn("sector1_percent",col("sector1.percent")).drop("sector1")\
    .withColumn("sector2_name",col("sector2.name")).withColumn("sector2_percent",col("sector2.percent")).drop("sector2")\
    .withColumn("sector3_name",col("sector3.name")).withColumn("sector3_percent",col("sector3.percent")).drop("sector3")\
    .withColumn("sector4_name",col("sector4.name")).withColumn("sector4_percent",col("sector4.percent")).drop("sector4")\
    .withColumn("sector_namecode",explode("sector_namecode")).withColumn("sector_namecode_code",col("sector_namecode.code"))\
    .withColumn("sector_namecode_name",col("sector_namecode.name")).drop("sector_namecode") \
    .withColumn("theme1Name", col("theme1.Name")).withColumn("theme1Percent", col("theme1.Percent")).drop("theme1")\
    .withColumn("theme_namecode", explode(col("theme_namecode"))).select(col("theme_namecode.*"), "*").drop("theme_namecode")\

df.printSchema()
df.show(truncate=False)

"""


