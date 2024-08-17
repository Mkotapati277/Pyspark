from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct,expr,udf
from pyspark.sql.functions import mean,col,split,to_date
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, col,aggregate,min,avg
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r''
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
# Initialize Spark session
spark = SparkSession.builder.appName("").getOrCreate()
# Sample data
data = [Row(UserID=4001, Age=17),
        Row(UserID=4002, Age=45),
        Row(UserID=4003, Age=65),
        Row(UserID=4004, Age=30),
        Row(UserID=4005, Age=80)]
# Create DataFrame
df = spark.createDataFrame(data)
df.show()

def cat_age(age):
    if age < 18: return 'youth'
    elif age < 60 : return 'adult'
    else : return 'senior'
age_udf = udf(cat_age,StringType())
df1 =df.withColumn('AgeGroup',age_udf(df["Age"]))
df1.show()
