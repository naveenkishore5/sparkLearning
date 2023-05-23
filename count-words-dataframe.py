from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('WordCountDf').getOrCreate()

inputFile = spark.read.text("file:///Users/naveenkishorek/Downloads/Book.txt")

words = inputFile.select(func.explode(func.split(inputFile.value, '\\W+')).alias('word'))
words.filter(words.word != "")

words = words.select(func.lower(words.word).alias('word'))

wordCount = words.groupBy('word').count()

wordCountSorted = wordCount.sort(wordCount['count'].desc())

wordCountSorted.show(30)

spark.stop()