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
#Problem: Given a dataset of sales records, identify and replace all missing values in the 'amount' column with the average sales amount.

sales_data = [("1", 100), ("2", 150), ("3", None), ("4", 200), ("5", None)]

# Creating DataFrame
sales_df = spark.createDataFrame(sales_data, ["sale_id", "amount"])
sales_df.show()
# Calculate the average sales amount
avg_amount = sales_df.na.drop().agg(mean(col("amount"))).first()[0]

# Replace missing values with the average amount
sales_df_filled = sales_df.na.fill(avg_amount)

# Show the result
sales_df_filled.show()
