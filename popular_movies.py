from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, FloatType

spark = SparkSession.builder.appName('popular_movies').getOrCreate()

schema = StructType([
                StructField('userID', IntegerType(), True),
                StructField('movieID', IntegerType(), True),
                StructField('rating', FloatType(), True),
                StructField('timestamp', LongType(), True)
])

moviesDF = spark.read.option('sep', ',').schema(schema).csv('/Users/naveenkishorek/Desktop/Naveen_learning/Spark_learning/ml-latest-small/ratings.csv')

topMoviesDf = moviesDF.groupBy('movieID').count().orderBy(func.desc('count'))

topMoviesDf.show()

spark.stop()


