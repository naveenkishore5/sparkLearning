from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

spark = SparkSession.builder.appName('min-max-temp-dataframe').getOrCreate()

schema = StructType([\
            StructField('stationId', StringType(), True),\
            StructField('date', IntegerType(), True),\
            StructField('measure_type', StringType(), True),\
            StructField('temperature', FloatType(), True)
    ])

df = spark.read.schema(schema).csv("file:///Users/naveenkishorek/Downloads/1800.csv")
df.printSchema()

min_temp = df.filter(df.measure_type == 'TMIN')

select_columns = min_temp.select('stationId', 'temperature')

minTempByStation = select_columns.groupBy('stationId').min('temperature')

maxTempByStation = select_columns.groupBy('stationId').max('temperature')

minTempByStation.show()
maxTempByStation.show()

spark.stop()