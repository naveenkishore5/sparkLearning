from pyspark.sql import SparkSession
from pyspark.sql import functions as func

movie_name_file_loc = '/Users/naveenkishorek/Desktop/Naveen_learning/Spark_learning/ml-latest-small/movies.csv'

spark = SparkSession.builder.appName('test').getOrCreate()

def loadMovieNames():
    movie_names = {}
    df = spark.read.csv(movie_name_file_loc, header=True, inferSchema=True)
    for f in df.rdd.collect():
        print(f[0], f[1])

loadMovieNames()
spark.stop()