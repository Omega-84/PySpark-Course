from pyspark import SparkConf, SparkContext
import operator
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)
def parser(line):
    fields = line.split(",")
    age = int(fields[2])
    num = int(fields[3])
    return (age,num)
lines = sc.textFile("E:\\Data Science Practice\\Udemy\\Spark_course\\fakefriends.csv")
rdd = lines.map(parser)
rdd = rdd.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y : (x[0] + y[0],x[1] + y[1]))
rdd = rdd.mapValues(lambda x : x[0]/x[1])
result = rdd.collect()
for i in sorted(result,key =operator.itemgetter(0)):
    print(f"Average friends for a {i[0]} year old is {int(i[1])}")
