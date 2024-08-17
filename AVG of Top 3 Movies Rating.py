from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import mean,col,split,to_date
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, col,aggregate,min,avg
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
data_movies = [(1, "Movie A"), (2, "Movie B"), (3, "Movie C"), (4, "Movie D"), (5, "Movie E")]
data_ratings = [(1, 101, 4.5), (1, 102, 4.0), (2, 103, 5.0),
                (2, 104, 3.5), (3, 105, 4.0), (3, 106, 4.0),
                (4, 107, 3.0), (5, 108, 2.5), (5, 109, 3.0)]
columns_movies = ["MovieID", "MovieName"]
columns_ratings = ["MovieID", "UserID", "Rating"]
df_movies = spark.createDataFrame(data_movies,columns_movies)
df_ratings =spark.createDataFrame(data_ratings,columns_ratings)
df_movies.show()
df_ratings.show()
avg_ratings = df_ratings.groupBy("MovieID").agg(avg("Rating").alias("Avg_Rating"))
avg_ratings.show()
top_3 = avg_ratings.join(df_movies,"MovieID","left").orderBy("Avg_Rating",ascending=False).limit(3)
top_3.show()
