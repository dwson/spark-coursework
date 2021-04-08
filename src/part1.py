from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, count, round


# read movies.csv and ratings.csv from the dataset_path and create a new dataset consists of users id with number of
# movies and genre he/she watched.
# return the dataset
def search_users_by_id(dataset_path: str, users: list):
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # filter dataset to get watched movie data of requested users and count the number of movies
    ratings_dataset_of_the_user = ratings_dataset.select("userId", "movieId").filter(col("userId").isin(users))
    movies_count_result = ratings_dataset_of_the_user.groupBy("userId") \
        .count() \
        .withColumnRenamed("count", "numOfMovies")

    # split genres of each movie
    split_movies_dataset = movies_dataset.select("movieId", "genres") \
        .withColumn("genres", explode(split(col("genres"), "\\|")))

    # merge with the users' watched movie info to get number of genres user watched
    genres_count_result = ratings_dataset_of_the_user \
        .join(split_movies_dataset, "movieId") \
        .dropDuplicates(["userId", "genres"]) \
        .groupBy("userId") \
        .count() \
        .withColumnRenamed("count", "numOfGenres")

    # get all genres user watched
    # genres_count_result.sort(genres_count_result["userId"].asc())

    result = movies_count_result.join(genres_count_result, "userId")

    return result.sort(result["userId"].asc())


# number of watches = count(rating), average of rating = avg(rating) from ratings.csv
# average of rating is rounded to 2 decimal places
def search_movie_by_id(dataset_path: str, n: int):
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")

    # cast String type column 'rating' to double type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("double"))

    result = ratings_dataset.where(col("movieId") == n) \
        .groupBy("movieId").agg(round(avg("rating"), 2).alias("avgRating"), count("rating").alias("numWatched"))

    return result


# read movies.csv and ratings.csv from the dataset_path and create a new dataset consists of movie names with highest
# rating.
# movie rating is calculated by adding all ratings from ratings.csv.
# return the dataset
def list_movies_by_rating(dataset_path: str, n: int):
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to integer type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("int"))

    result = ratings_dataset.groupBy("movieId") \
        .sum("rating") \
        .join(movies_dataset, ratings_dataset["movieId"] == movies_dataset["movieId"])

    result = result.sort(result["sum(rating)"].desc()) \
        .limit(n)

    return result.select(col("title"), col("sum(rating)"))


# number of watches = count(movieId) from ratings.csv
def list_movies_by_watches(dataset_path: str, n: int):
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to integer type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("int"))

    result = ratings_dataset.groupBy("movieId") \
        .count() \
        .join(movies_dataset, ratings_dataset["movieId"] == movies_dataset["movieId"])

    result = result.sort(result["count"].desc()) \
        .limit(n)

    return result.select(col("title"), col("count"))
