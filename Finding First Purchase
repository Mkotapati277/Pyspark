from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import mean,col,split,to_date
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, col,aggregate,min
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
purchase_data = [
    Row(UserID=1, PurchaseDate='2023-01-05'),
    Row(UserID=1, PurchaseDate='2023-01-10'),
    Row(UserID=2, PurchaseDate='2023-01-03'),
    Row(UserID=3, PurchaseDate='2023-01-12')]

df_purchase = spark.createDataFrame(purchase_data)
df_purchase = df_purchase.withColumn("PurchaseDate",col("PurchaseDate").cast("date"))
df_purchase.show()

first_purchase = df_purchase.groupby("UserID").agg(min("PurchaseDate"))
first_purchase.show()
