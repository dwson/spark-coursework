from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, count, round, lower


def search_users_by_id(dataset_path: str, users: list):
    """
    Searches the number of movies and genres that a given user or a group of users watched.

    The number of movies watched are calculated by counting the number of movies that the user rated.
    The number of genres watched are calculated by counting all the genres of the movie user rated.
    The genres are counted without duplication by splitting each movie genres to individual rows.

    :param dataset_path: The path of the dataset
    :param users: The list of users
    :return: The dataset of number of movies and genres that users watched
    """
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

    result = movies_count_result.join(genres_count_result, "userId")

    return result.sort(result["userId"].cast("int").asc())


def search_movie_by_id(dataset_path: str, n: int):
    """
    Searches the movie by id, calculate average rating and number of users watched the movie.

    The average rating of the movie is calculated by averaging values of rating column which are related to the movie.
    The number of users watched are calculated by counting all the users rated the movie.

    :param dataset_path: The path of the dataset
    :param n: The movie id
    :return: The dataset of average rating and number of users watched the movie
    """
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to double type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("double"))

    # calculate average rating and count number of watched users
    # average of rating is rounded to 2 decimal places
    result = ratings_dataset.where(col("movieId") == n) \
        .join(movies_dataset, "movieId") \
        .groupBy("movieId", "title") \
        .agg(round(avg("rating"), 2).alias("avgRating"), count("rating").alias("numWatched"))

    return result


def search_movie_by_title(dataset_path: str, title: str):
    """
    Searches the movie by title, calculate average rating and number of users watched the movie.

    The movie will be searched by checking if the movie title contains the given value.
    The average rating of the movie is calculated by averaging values of rating column which are related to the movie.
    The number of users watched are calculated by counting all the users rated the movie.

    All the movies containing the given title are searched together.
    e.g., if user searched "toy", the result will contain "Toy Story", "Babes is Toyland", etc.

    :param dataset_path: The path of the dataset
    :param title: The title of the movie
    :return: The dataset of average rating and number of users watched the movie
    """
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to double type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("double"))

    # filter the dataset by checking if a movie title contains the given value
    filtered_movies_dataset = movies_dataset.filter(lower(movies_dataset.title).like("%" + title.lower() + "%"))
    filtered_movies_ratings_dataset = filtered_movies_dataset.join(ratings_dataset, "movieId")

    # calculate average rating and count number of watched users
    # average of rating is rounded to 2 decimal places
    result = filtered_movies_ratings_dataset.groupBy("movieId", "title") \
        .agg(round(avg("rating"), 2).alias("avgRating"), count("rating").alias("numWatched"))

    return result.sort(result["movieId"].cast("int").asc())


def search_genre(dataset_path: str, genres: list):
    """
    Searches movies by given list of genres

    The functionality searches for each genre and stores the result data of the genre in separate datasets.
    The all movies in that genre are searched by checking if the given genre is contained in genres column of the movie.

    :param dataset_path: The path of the dataset
    :param genres: The list of genres
    :return: The list that contains the list containing name of genres and
             the list containing datasets of each genres
    """
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")
    genres_lowercase = [genre.lower() for genre in genres]

    # split genres of each movie
    split_genre_dataset = movies_dataset.select("movieId", "title", "genres") \
        .withColumn("genres", explode(split(col("genres"), "\\|")))

    # filter to get data of given genres
    filtered_genre_dataset = split_genre_dataset.filter(lower(col("genres")).isin(genres_lowercase))

    # split dataset to datasets of each genre
    list_of_genres = filtered_genre_dataset.select("genres").distinct().rdd.flatMap(lambda x: x).collect()
    genre_datasets_array = [filtered_genre_dataset.where(filtered_genre_dataset["genres"] == genre) for genre in
                            list_of_genres]

    # remove genres column in each dataset
    for i in range(len(genre_datasets_array)):
        genre_datasets_array[i] = genre_datasets_array[i].drop("genres")

    result = [list_of_genres, genre_datasets_array]

    return result


def search_movie_by_year(dataset_path: str, year: str):
    """
    Searches movies by year.

    The movies related to the year are searched by checking if the given year is contained in title column.

    :param dataset_path: The path of the dataset
    :param year: The year
    :return: The dataset of movies related to the given year
    """
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # filter the dataset to get movies related to the given year
    # average of rating is rounded to 2 decimal places
    result = movies_dataset \
        .filter(lower(movies_dataset.title).like("%(" + year + ")%")) \
        .select("movieId", "title")

    return result.sort(result["movieId"].asc())


def list_movies_by_rating(dataset_path: str, n: int):
    """
    Lists the movies by rating.

    The movie rating is calculated by adding all ratings of the movie.

    :param dataset_path: The path of the dataset
    :param n: The number of columns to return, top n rated movies
    :return: The dataset of the top rated movies
    """
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to double type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("double"))

    result = ratings_dataset.groupBy("movieId") \
        .sum("rating") \
        .join(movies_dataset, ratings_dataset["movieId"] == movies_dataset["movieId"])

    result = result.sort(result["sum(rating)"].desc()) \
        .limit(n)

    return result.select(col("title"), col("sum(rating)"))


# number of watches = count(movieId) from ratings.csv
def list_movies_by_watches(dataset_path: str, n: int):
    """
    Lists the movies by number of users watched.

    The number of users watched the movie is calculated by counting the number of users that rated the movie.

    :param dataset_path: The path of the dataset
    :param n: The number of columns to return, top n rated movies
    :return: The dataset of the top rated movies
    """
    # set local[*] to utilize all cores
    spark_session = SparkSession.builder.master("local[*]").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to double type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("double"))

    result = ratings_dataset.groupBy("movieId") \
        .count() \
        .join(movies_dataset, ratings_dataset["movieId"] == movies_dataset["movieId"])

    result = result.sort(result["count"].desc()) \
        .limit(n)

    return result.select(col("title"), col("count"))
