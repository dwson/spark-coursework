from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def compare_movie_tastes(dataset_path: str, users: list):
    None