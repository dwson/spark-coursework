import os

ml_latest_path = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest/"
ml_latest_small_path = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest-small/"

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("App")
sc = SparkContext(conf = conf)
lines = sc.textFile(ml_latest_small_path + "links.csv") # Create an RDD called lines
print(lines.count()) # Count the number of items in this RDD
print(lines.first()) # 1st item in RDD, i.e. 1st line of README.md