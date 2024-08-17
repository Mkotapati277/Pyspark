from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import mean,col,split,to_date
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, col,aggregate
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
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
data = [Row(Date='2023-01-01', ProductID=100, QuantitySold=10),
        Row(Date='2023-01-02', ProductID=100, QuantitySold=15),
        Row(Date='2023-01-03', ProductID=100, QuantitySold=20),
        Row(Date='2023-01-04', ProductID=100, QuantitySold=25),
        Row(Date='2023-01-05', ProductID=100, QuantitySold=30),
        Row(Date='2023-01-06', ProductID=100, QuantitySold=35),
        Row(Date='2023-01-07', ProductID=100, QuantitySold=40),
        Row(Date='2023-01-08', ProductID=100, QuantitySold=45)]
df_sales = spark.createDataFrame(data)
df_sales.show()
df_sales = df_sales.withColumn("Date", F.to_date(F.col("Date")))
df_sales.show()
# Window specification for 7-day rolling average
windowSpec = Window.partitionBy('ProductID').orderBy('Date').rowsBetween(-6, 0)
# Calculating the rolling average
rollingAvg = df_sales.withColumn('7DayAvg', F.avg('QuantitySold').over(windowSpec))
# Show results
rollingAvg.show()
