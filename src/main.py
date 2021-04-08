#!/usr/bin/python
import argparse
import os
import part1
import part2

from datetime import datetime

DATASET_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest/"  # large one
# DATASET_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest-small/"  # small one
OUTPUT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../output/"

AVAILABLE_ARGS = ["-search-user-id", "-search-movie-id", "-search-movie-title", "-search-genre", "-search-year",
                  "-list-rating", "-list-watches", "-find-favourite-genre", "-compare-movie-tastes"]

USAGE = "spark-submit main.py {[OPTIONS] <value>}"


# Store a given dataset into OUTPUT_PATH with a current timestamp
# e.g., 01.04.2021-19:42:28 -search-user-id 1,2,3
def store_dataset(dataset, filename):
    file_path = OUTPUT_PATH + datetime.today().strftime("%d.%m.%Y-%H:%M:%S-") + filename
    dataset.write.option("header", "true").csv(file_path)
    print("Result saved in " + file_path)


def is_every_element_int(var):
    try:
        for element in var:
            int(element)
        return True
    except ValueError:
        return False


def main():
    parser = argparse.ArgumentParser(usage=USAGE)

    for arg in AVAILABLE_ARGS:
        parser.add_argument(arg)

    args = parser.parse_args()

    # we've decided to use SparkSession instead of SparkContext because it's newer and supports more methods
    # spark_session = SparkSession.builder.master("local").appName("App").getOrCreate()
    # dataset = spark_session.read.csv(DATASET_PATH + "links.csv")

    for arg in vars(args):
        value = getattr(args, arg)

        if value is not None:
            print("Argument:", arg, value)

            if "search_user_id" in arg:
                None  # TODO
            elif "search_movie_id" in arg:
                None  # TODO
            elif "search_movie_title" in arg:
                None  # TODO
            elif "search_genre" in arg:
                None  # TODO
            elif "search_year" in arg:
                None  # TODO
            elif "list_rating" in arg:
                try:
                    result = part1.list_movies_by_rating(DATASET_PATH, int(value))
                    result.show(truncate=False)
                    store_dataset(result, arg + '-' + value)
                except ValueError:
                    print("The value must be a number:", value)
                    print("e.g., -list-rating 10")
            elif "list_watches" in arg:
                try:
                    result = part1.list_movies_by_watches(DATASET_PATH, int(value))
                    result.show(truncate=False)
                    store_dataset(result, arg + "-" + value)
                except ValueError:
                    print("The value must be a number:", value)
                    print("e.g., -list-watches 10")
            elif "find_favourite_genre" in arg:
                try:
                    users = value.split(',')

                    # allows a user or a group of users
                    if len(users) > 0 and is_every_element_int(users):
                        result = part2.find_favourite_genre(DATASET_PATH, users)

                        if result is not None:
                            result.show(truncate=False)
                            store_dataset(result, arg + '-' + value)
                    else:
                        raise ValueError
                except ValueError:
                    print("The value must be a number or numbers separated by a comma:", value)
                    print("e.g., -find-favourite-genre 1")
                    print("e.g., -find-favourite-genre 1,2,3")
            elif "compare_movie_tastes" in arg:
                try:
                    users = value.split(',')

                    # type check
                    if len(users) == 2 and is_every_element_int(users):
                        result = part2.compare_movie_tastes(DATASET_PATH, users)

                        if result is not None:
                            result.show(truncate=False)
                            store_dataset(result, arg + '-' + value)
                    else:
                        raise ValueError
                except ValueError:
                    print("The value must be two numbers separated by a comma:", value)
                    print("e.g., -compare-movie-tastes 1,2")


if __name__ == "__main__":
    main()
