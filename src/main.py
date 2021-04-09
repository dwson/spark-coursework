#!/usr/bin/python
import argparse
import os
import part1
import part2

from datetime import datetime

# DATASET_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest/"  # large one
DATASET_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../ml-latest-small/"  # small one
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
                try:
                    users = value.split(',')

                    # type check
                    if is_every_element_int(users):
                        result = part1.search_users_by_id(DATASET_PATH, users)

                        if result is not None:
                            result.show(truncate=False)
                            store_dataset(result, arg + '-' + value)
                    else:
                        raise ValueError
                except ValueError:
                    print("The value must be one or more numbers separated by a comma:", value)
                    print("e.g., -search-user-id 1,2,5")
            elif "search_movie_id" in arg:
                try:
                    result = part1.search_movie_by_id(DATASET_PATH, int(value))
                    result.show(truncate=False)
                    store_dataset(result, arg + '-' + value)
                except ValueError:
                    print("The value must be a number:", value)
                    print("e.g., -search-movie-id 10")
            elif "search_movie_title" in arg:
                try:
                    result = part1.search_movie_by_title(DATASET_PATH, value)
                    result.show(truncate=False)
                    store_dataset(result, arg + '-' + value)
                except ValueError:
                    print("The value must be a single string:", value)
                    print("e.g., -search-movie-title \"toy story\"")
            elif "search_genre" in arg:
                try:
                    genres = value.split(',')

                    results = part1.search_genre(DATASET_PATH, genres)

                    list_of_genre = results[0]
                    dataset_by_genre = results[1]

                    for i in range(len(list_of_genre)):
                        print("Genre: ", list_of_genre[i])
                        dataset_by_genre[i].show(truncate=False)
                        store_dataset(dataset_by_genre[i], arg + '-' + value + " (" + list_of_genre[i] + ")")

                except ValueError:
                    print("The value must be one or more string:", value)
                    print("e.g., -search-genre Adventure,Animation")
            elif "search_year" in arg:
                try:
                    if len(value) == 4:
                        result = part1.search_movie_by_year(DATASET_PATH, value)
                        result.show(truncate=False)
                        store_dataset(result, arg + '-' + value)
                    else:
                        raise ValueError
                except ValueError:
                    print("The value must be a year (4 digit number):", value)
                    print("e.g., -search-year 1994")
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
