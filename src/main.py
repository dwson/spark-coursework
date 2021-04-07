#!/usr/bin/python
# how to run this on command line?
# ../spark-3.1.1-bin-hadoop3.2/bin/spark-submit main.py
import argparse
import os
import list

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from datetime import datetime

# DATASET_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest/" # large one
DATASET_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest-small/"  # small one
OUTPUT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../output/"

AVAILABLE_ARGS = ["-search-user-id", "-search-movie-id", "-search-movie-title", "-search-genre", "-search-year",
                  "-list-rating", "-list-watches", "-find-favourite-genre", "-compare-movie-tastes"]

USAGE = "spark-submit main.py [OPTIONS]"


# Store a given dataset into OUTPUT_PATH with a current timestamp
# e.g., 01.04.2021-19:42:28 -search-user-id 1,2,3
def store_dataset(dataset, filename):
    file_path = OUTPUT_PATH + datetime.today().strftime("%d.%m.%Y-%H:%M:%S-") + filename
    dataset.write.option('header', 'true').csv(file_path)
    print("Result saved in " + file_path)


def main():
    parser = argparse.ArgumentParser(usage=USAGE)

    for arg in AVAILABLE_ARGS:
        parser.add_argument(arg)

    args = parser.parse_args()

    # starter code
    # conf = SparkConf().setMaster("local").setAppName("App")
    # sc = SparkContext(conf=conf)
    # lines = sc.textFile(ML_LATEST_SMALL_PATH + "links.csv")  # Create an RDD called lines
    # print(lines.count())  # Count the number of items in this RDD
    # print(lines.first())  # 1st item in RDD, i.e. 1st line of README.md

    # we've decided to use SparkSession instead of SparkContext because it's newer and supports more methods
    # spark_session = SparkSession.builder.master("local").appName("App").getOrCreate()
    # dataset = spark_session.read.csv(DATASET_PATH + "links.csv")

    for arg in vars(args):
        value = getattr(args, arg)

        if value is not None:
            print("Found '", arg, "' - '", value, "'")

            if 'search_user_id' in arg:
                None  # TODO
            elif 'search_movie_id' in arg:
                None  # TODO
            elif 'search_movie_title' in arg:
                None  # TODO
            elif 'search_genre' in arg:
                None  # TODO
            elif 'search_year' in arg:
                None  # TODO
            elif 'list_rating' in arg:
                try:
                    result = list.list_movies_by_rating(DATASET_PATH, int(value))
                    result.show(truncate=False)
                    store_dataset(result, arg + "-" + value)
                except ValueError:
                    print("The value must be a number: ", value)
            elif 'list_watches' in arg:
                None  # Todo
            elif 'find_favourite_genre' in arg:
                None  # TODO
            elif 'compare_movie_tastes' in arg:
                None  # TODO


if __name__ == "__main__":
    main()
