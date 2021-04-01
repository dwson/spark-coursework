#!/usr/bin/python
# how to run this on command line?
# ../spark-3.1.1-bin-hadoop3.2/bin/spark-submit main.py
import argparse
import sys, os, getopt
from pyspark import SparkConf, SparkContext

ml_latest_path = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest/"
ml_latest_small_path = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest-small/"

AVAILABLE_ARGS = ["-search-user-id", "-search-movie-id", "-search-movie-title", "-search-genre", "-search-year",
               "-list-rating", "-list-watches", "-find-favourite-genre", "-compare-movie-tastes"]

USAGE = "./spark-submit main.py [OPTIONS]"


def main():
    parser = argparse.ArgumentParser(usage=USAGE)

    for arg in AVAILABLE_ARGS:
        parser.add_argument(arg)

    args = parser.parse_args()

    for arg in vars(args):
        value = getattr(args, arg)

        if value is not None:
            print("Found [", arg, "] - ", value)

            if 'search_user_id' in arg:
                None # TODO
            elif 'search_movie_id' in arg:
                None # TODO
            elif 'search_movie_title' in arg:
                None # TODO
            elif 'search_genre' in arg:
                None # TODO
            elif 'search_year' in arg:
                None # TODO
            elif 'list_rating' in arg:
                None # TODO
            elif 'list_watches' in arg:
                None # TODO
            elif 'find_favourite_genre' in arg:
                None # TODO
            elif 'compare_movie_tastes' in arg:
                None # TODO

    # starter code
    conf = SparkConf().setMaster("local").setAppName("App")
    sc = SparkContext(conf=conf)
    lines = sc.textFile(ml_latest_small_path + "links.csv")  # Create an RDD called lines
    print(lines.count())  # Count the number of items in this RDD
    print(lines.first())  # 1st item in RDD, i.e. 1st line of README.md


if __name__ == "__main__":
    main()
