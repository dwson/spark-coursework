from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import collect_list


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
