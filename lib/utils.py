import configparser

from pyspark import SparkConf
from pyspark.sql.functions import col, count, mean, stddev


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


def advanced_stats(df):
    pass
    # print("----------------------------- Numerical columns --------------------------------")
    # num_cols = [c for c, t in df.dtypes if t in ("int", "double", "float")]
    # res = df.select([mean(col(c)).alias(f"moyenne_{c}") for c in num_cols] + [
    #                stddev(col(c)).alias(f"stddev_{c}") for c in num_cols])
    # res.show()
    # print("----------------------------- Categorical columns --------------------------------")
    # cat_cols = [c for c, t in df.dtypes if t == "string"]
    # for col_name in cat_cols:
    #    print(f"\nValeurs uniques pour {col_name}:")
    #    # print the 10 first unique values
    #    df.select(col_name).distinct().show(10, False)


def corr_matrix(df):
    pass
#    num_cols = [c for c, t in df.dtypes if t in ("int", "double", "float")]
#    correlation_matrix = {}
#    for i in range(len(num_cols)):
#        for j in range(i + 1, len(num_cols)):
#            col1, col2 = num_cols[i], num_cols[j]
#            correlation = df.stat.corr(col1, col2)
#            correlation_matrix[f"{col1} - {col2}] "] = correlation
#
#    for pair, cor in correlation_matrix.items():
#        print(f"Corrélation entre {pair}: {cor}")
