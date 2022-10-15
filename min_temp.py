from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("MinTemp")
sc = SparkContext(conf=conf)
def parser(line):
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    temp = int(fields[3]) * 0.1
    return (stationId,entryType,temp)
lines = sc.textFile("E:\\Data Science Practice\\Udemy\\Spark_course\\1800.csv")
rdd = lines.map(parser)
rdd = rdd.filter(lambda x : 'TMIN' in x[1])
rdd = rdd.map(lambda x : (x[0],x[2]))
rdd = rdd.reduceByKey(lambda x,y : min(x,y)).collect()
for i in rdd:
    print(f"The mininum temperature for station {i[0]} was {i[1]} C.")