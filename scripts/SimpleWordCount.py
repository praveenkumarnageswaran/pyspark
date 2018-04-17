import subprocess
some_path = "/apps/hive/warehouse/samples/wordcount/sampleOut"
subprocess.call(["hadoop", "fs", "-rm", "-f", "-r", "-skipTrash", some_path])

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Pyspark Word Count")
sc = SparkContext(conf = conf)
contentRDD =sc.textFile("/apps/hive/warehouse/samples/wordcount/newSample.txt")
nonempty_lines = contentRDD.filter(lambda x: len(x) > 0)
words = nonempty_lines.flatMap(lambda x: x.split(' '))
wordcount = words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(False)
for word in wordcount.collect():
	print(word)
wordcount.saveAsTextFile("/apps/hive/warehouse/samples/wordcount/sampleOut")
