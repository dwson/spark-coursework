from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# read movies.csv and ratings.csv from the dataset_path and create a new dataset consists of movie names with highest
# rating.
# movie rating is calculated by adding all ratings from ratings.csv.
# return the dataset
def list_movies_by_rating(dataset_path: str, n: int):
    spark_session = SparkSession.builder.master("local").appName("App").getOrCreate()

    ratings_dataset = spark_session.read.options(header='True').csv(dataset_path + "ratings.csv")
    movies_dataset = spark_session.read.options(header='True').csv(dataset_path + "movies.csv")

    # cast String type column 'rating' to integer type for calculation
    ratings_dataset = ratings_dataset.withColumn("rating", ratings_dataset["rating"].cast("int"))

    joined_dataset = ratings_dataset.groupBy("movieId") \
        .sum("rating") \
        .join(movies_dataset, ratings_dataset["movieId"] == movies_dataset["movieId"])

    joined_dataset = joined_dataset.sort(joined_dataset["sum(rating)"].desc()) \
        .limit(n)

    return joined_dataset.select(col("title"), col("sum(rating)"))


def list_movies_by_watches(dataset_path: str, n: int):
    spark_session = SparkSession.builder.master("local").appName("App").getOrCreate()
    dataset = spark_session.read.csv(dataset_path + "links.csv")

    return dataset
