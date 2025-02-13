import configparser

from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, value) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)
    return spark_conf


def load_survey_df(spark_session, datafile_path):
    df = spark_session.read \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .csv(datafile_path)
    return df


def count_by_country(df):
    count_df = df.where("Age <40") \
        .select("Age", "Gender", "Country", "state") \
        .groupby("Country") \
        .count()
    return count_df
