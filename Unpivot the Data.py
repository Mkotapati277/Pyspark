from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import mean,col,split,to_date
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, col,aggregate
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
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
data = [("Product1", 100, 150, 200),
        ("Product2", 200, 250, 300),
        ("Product3", 300, 350, 400)]
# Columns: Product, Sales_Jan, Sales_Feb, Sales_Mar
columns = ["Product", "Sales_Jan", "Sales_Feb", "Sales_Mar"]

df = spark.createDataFrame(data, columns)
df.show()
pivot_data = df.selectExpr("Product","stack(3,'jan',Sales_Jan,'feb',Sales_Feb,'mar',Sales_Mar) as (Month,Sales)")
pivot_data.show()
