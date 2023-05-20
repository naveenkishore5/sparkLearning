from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/naveenkishorek/Downloads/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps_min = minTemps.map(lambda x: (x[0], x[2]))
stationTemps_max = maxTemps.map(lambda x: (x[0], x[2]) )
minTemps = stationTemps_min.reduceByKey(lambda x, y: min(x,y))
maxTemps = stationTemps_max.reduceByKey(lambda x, y: min(x,y))
results_min = minTemps.collect()
results_max = maxTemps.collect()

print('Minimum Temp')
for result in results_min:
    print(result[0] + "\t{:.2f}F".format(result[1]))
print('Maximum Temp')
for res in results_max:
    print(res[0] + "\t{:.2f}F".format(res[1]))