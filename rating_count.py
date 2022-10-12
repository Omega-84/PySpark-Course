from pyspark import SparkConf, SparkContext
import collections
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
lines = sc.textFile("E:\\Data Science Practice\\Udemy\\Spark_course\\ml-100k\\u.data",use_unicode=False)
ratings = lines.map(lambda x : x.split()[2])
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key,value in sortedResults.items():
    print(f"Rating = {key}, counts = {value}")