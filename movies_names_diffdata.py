import codecs
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, FloatType

# encoding = 'ISO-8859-1'
movie_name_file_loc = '/Users/naveenkishorek/Desktop/Naveen_learning/Spark_learning/ml-latest-small/movies.csv'
rating_file_loc = '/Users/naveenkishorek/Desktop/Naveen_learning/Spark_learning/ml-latest-small/ratings.csv'

def loadMovieNames():
    movie_names = {}
    files = spark.read.csv(movie_name_file_loc, header=True, inferSchema=True)
    # with codecs.open(movie_name_file_loc, 'r', errors='ignore') as files:
    # header = next(files)
    for fields in files.rdd.collect():
        movie_names[int(fields[0])] = fields[1]
        
    return movie_names

spark = SparkSession.builder.appName('combine_different_dataset').getOrCreate()

schema = StructType([
                StructField('userID', IntegerType(), True),
                StructField('movieID', IntegerType(), True),
                StructField('rating', FloatType(), True),
                StructField('timestamp', LongType(), True)
])

moviesDF = spark.read.option('sep', ',').schema(schema).csv(rating_file_loc)


movieDictionary = spark.sparkContext.broadcast(loadMovieNames())

topMoviesDf = moviesDF.groupBy('movieID').count().orderBy(func.desc('count'))

def lookup_name(movieID):
    if movieID in movieDictionary.value.keys():
        return movieDictionary.value[movieID]

lookupNameUDF = func.udf(lookup_name)

movies_with_names = topMoviesDf.withColumn('movieTitle', lookupNameUDF(func.col('movieID')))

sorted_Movies_By_Names = movies_with_names.orderBy(func.desc('count'))

sorted_Movies_By_Names.show(10, False)

spark.stop()