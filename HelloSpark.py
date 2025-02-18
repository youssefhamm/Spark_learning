import sys
from lib.utils import get_spark_app_config, load_survey_df, count_by_country, advanced_stats
from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/path/to/log4j.properties") \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.roDebugString())

    # Read the CSV file
    survey_df = load_survey_df(spark, "data/sample.csv")
    partitioned_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partitioned_survey_df)

    # Advanced stats
    advanced_stats(survey_df)

    logger.info(count_df.collect())

    input("Press Enter")
    logger.info("Finished HelloSpark")
    # spark.stop()
