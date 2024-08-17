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
#find the missing value from the list

# Sample data
data = [(1,), (2,), (4,), (5,), (7,), (8,), (10,)]
df_numbers = spark.createDataFrame(data, ["Number"])
df_numbers.show()
df1 = spark.range(1,12).toDF("Number")
missing_number =df1.join(df_numbers,"Number","leftanti")
missing_number.show()
