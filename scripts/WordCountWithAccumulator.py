from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Pyspark Word Count")
sc = SparkContext(conf = conf)
contentRDD =sc.textFile("/apps/hive/warehouse/samples/wordcount/newSample.txt")
nonempty_lines = contentRDD.filter(lambda x: len(x) > 0)
words = nonempty_lines.flatMap(lambda x: x.split(' '))
accum = sc.accumulator(0)
words.foreach(lambda x: accum.add(1))
word_count = accum.value
print "There are",word_count,"words in the source file"
