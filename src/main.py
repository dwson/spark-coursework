# how to run this on command line?
# ../spark-3.1.1-bin-hadoop3.2/bin/spark-submit main.py

import os
from pyspark import SparkConf, SparkContext

ml_latest_path = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest/"
ml_latest_small_path = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest-small/"

# sample code from
# https://studres.cs.st-andrews.ac.uk/CS5052/Examples/Spark%20Tutorial%20Questions%20Only.pdf

conf = SparkConf().setMaster("local").setAppName("App")
sc = SparkContext(conf=conf)
lines = sc.textFile(ml_latest_small_path + "links.csv")  # Create an RDD called lines
print(lines.count())  # Count the number of items in this RDD
print(lines.first())  # 1st item in RDD, i.e. 1st line of README.md
