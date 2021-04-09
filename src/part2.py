from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def find_favourite_genre(dataset_path: str, users: list):
    """
    Finds the favourite genre of a given user or a group of users.

    The favourite genre is calculated by counting the number of occurrences of a genre that the given user(s) rated.
    The genre with the highest occurrence will be selected.

    :param dataset_path: The path of the dataset
    :param users: The list of users
    :return: The dataset
    """
    # set Spark local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    ratings_of_the_users = ratings_dataset.select("userId", "movieId").filter(col("userId").isin(users))

    # split genres
    genres_dataset = movies_dataset.select("movieId", "genres") \
        .withColumn("genres", explode(split(col("genres"), "\\|")))

    # filter unrelated movies
    genres_dataset = genres_dataset.join(ratings_of_the_users,
                                         genres_dataset["movieId"] == ratings_of_the_users["movieId"],
                                         "leftsemi") \
        .groupBy("genres") \
        .count()

    max_appearance_count = genres_dataset.sort(genres_dataset["count"].desc()).first()["count"]
    favourite_genres_dataset = genres_dataset.where(col("count") == max_appearance_count)

    return favourite_genres_dataset


def compare_movie_tastes(dataset_path: str, users: list):
    """
    Finds the tags of given users.

    We assumed "movie tastes" means tags.
    This function will find given users from tags.csv and summarize their tags in a single data frame.

    :param dataset_path: The path of the dataset
    :param users: The list of users
    :return: The dataset
    """
    # set Spark local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    tags_dataset = spark_session.read.options(header='True').csv(dataset_path + "tags.csv")
    user0_tags_dataset = tags_dataset.where(col("userId") == users[0])
    user1_tags_dataset = tags_dataset.where(col("userId") == users[1])

    if user0_tags_dataset.count() > 0 and user1_tags_dataset.count() > 0:
        user0_tags_dataset = user0_tags_dataset.groupBy("userId") \
            .agg(concat_ws(", ", collect_list(user0_tags_dataset["tag"])))
        user1_tags_dataset = user1_tags_dataset.groupBy("userId") \
            .agg(concat_ws(", ", collect_list(user1_tags_dataset["tag"])))

        result = user0_tags_dataset.union(user1_tags_dataset)

        return result.withColumnRenamed("concat_ws(, , collect_list(tag))", "movieTastes")
    else:
        print("Not enough user data")
        print("Number of tags of user", users[0], ':', user0_tags_dataset.count())
        print("Number of tags of user", users[1], ':', user1_tags_dataset.count())

        return None
