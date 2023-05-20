from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:///Users/naveenkishorek/Downloads/Book.txt")
words = input.flatMap(normalize_words)
wordCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y:x+y)
wordCountsSorted = wordCount.map(lambda x: (x[1], x[0])).sortByKey()
# wordCounts = words.countByValue()
result = wordCountsSorted.collect()


for res in result:
    count = str(res[0])
    cleanWord = res[1].encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + count)
