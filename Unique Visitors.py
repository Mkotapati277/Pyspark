
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
visitor_data = [Row(Date='2023-01-01', VisitorID=101),
                Row(Date='2023-01-01', VisitorID=102),
                Row(Date='2023-01-01', VisitorID=101),
                Row(Date='2023-01-02', VisitorID=103),
                Row(Date='2023-01-02', VisitorID=101)]
# Create DataFrame
df_visitors = spark.createDataFrame(visitor_data)
df_visitors.show()
vd1 = df_visitors.withColumn("Date",col("Date").cast("date"))
unique_vis = df_visitors.groupBy("Date").agg(countDistinct("VisitorID").alias("Unique_visitors"))
unique_vis.show()
