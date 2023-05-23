from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

spark = SparkSession.builder.appName('customer-words').getOrCreate()

schema = StructType([ \
                StructField('customerId', IntegerType(), True),
                StructField('orderId', StringType(), True),
                StructField('amount', FloatType(), True)
                ])

inputrdd = spark.sparkContext.textFile("file:///Users/naveenkishorek/Downloads/customer-orders.csv")


splitrdd = inputrdd.map(lambda x: x.split(','))

split_schema_rdd = splitrdd.map(lambda x: (int(x[0]), x[1], float(x[2])))

df = spark.createDataFrame(split_schema_rdd, schema)

df.groupBy('customerId').sum().show()

total_customer_spent = df.groupBy('customerId').agg(func.round(func.sum('amount'), 2).alias('total_amount'))

total_customer_spent = total_customer_spent.sort(total_customer_spent['total_amount'].desc())

total_customer_spent.show()

spark.stop()