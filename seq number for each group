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
group_data = [
    Row(GroupID='A', Date='2023-01-01'),
    Row(GroupID='A', Date='2023-01-02'),
    Row(GroupID='B', Date='2023-01-01'),
    Row(GroupID='B', Date='2023-01-03')]
df_group = spark.createDataFrame(group_data)
df_group.show()
df_group = df_group.withColumn("Date",col("Date").cast("date"))
wind_spec = Window.partitionBy("GroupID").orderBy("Date")
seq_data = df_group.withColumn("seq",row_number().over(wind_spec).alias("seq"))

seq_data.show()
