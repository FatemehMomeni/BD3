import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    sc = SparkContext("local","Spark Word Count")
    words = sc.textFile("/user/ebrahimi/hw3-data/file1G.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
    wordCounts.saveAsTextFile("/user/fatemeh_momeni/spark_output")
