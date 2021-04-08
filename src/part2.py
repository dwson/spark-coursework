from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import col, explode, split, avg, count, round


def find_favourite_genre(dataset_path: str, users: list):
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # filter dataset to get watched movie data of requested users and count the number of movies
    ratings_of_the_users = ratings_dataset.select("userId", "movieId").filter(col("userId").isin(users))

    # split genres of each movie
    genres_dataset = movies_dataset.select("movieId", "genres") \
        .withColumn("genres", explode(split(col("genres"), "\\|")))

    # filter unselected movies
    genres_dataset = genres_dataset.join(ratings_of_the_users,
                                         genres_dataset["movieId"] == ratings_of_the_users["movieId"],
                                         "leftsemi") \
        .groupBy("genres") \
        .count()

    max_appearance_count = genres_dataset.sort(genres_dataset["count"].desc()).first()["count"]
    favourite_genres_dataset = genres_dataset.where(col("count") == max_appearance_count)

    return favourite_genres_dataset


def compare_movie_tastes(dataset_path: str, users: list):
    # set local[*] to utilize all cores
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
