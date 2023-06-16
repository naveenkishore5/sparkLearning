from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('CustomerOrder')
sc = SparkContext(conf= conf)
def parse_input(text):
    fields = text.split(',')
    cus_id = int(fields[0])
    item_id = int(fields[1])
    amount_spent = float(fields[2])
    return (cus_id, item_id, amount_spent)

lines = sc.textFile("file:///Users/naveenkishorek/Downloads/customer-orders.csv")
customer = lines.map(parse_input)
rdd = customer.map(lambda x:(x[0], x[2]))
rdd2 = rdd.reduceByKey(lambda x, y: x + y).sortByKey()
result = rdd2.collect()

for res in result:
    print(res)


