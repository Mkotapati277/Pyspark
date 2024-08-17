from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import mean,col,split
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
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
# different delimiter in a file pyspark

# Sample data
data = ["1,Alice\t30|New York"]

df = spark.createDataFrame(data,"string")
df.show()

split_col = split(df["value"],",|\t|\|")

df1 =(df.withColumn("id",split_col.getItem(0)).withColumn("name",split_col.getItem(1))
      .withColumn("age",split_col.getItem(2)).withColumn("city",split_col.getItem(3))
      .drop("value"))

df1.show()
