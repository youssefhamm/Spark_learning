import sys
from lib.utils import get_spark_app_config, load_survey_df, count_by_country, advanced_stats, corr_matrix
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

    # Correlation betweeen numerical columns and correlation matrix
    numeric_columns = [
        c for c, t in survey_df.dtypes if t in ['int', 'double']]
    correlation_matrix = {}
    for i in range(len(numeric_columns)):
        for j in range(i + 1, len(numeric_columns)):
            col1, col2 = numeric_columns[i], numeric_columns[j]
            correlation = survey_df.stat.corr(col1, col2)
            correlation_matrix[f"{col1} - {col2}"] = correlation
    # 10. Afficher les corrélations
    for pair, correlation in correlation_matrix.items():
        print(f"Corrélation entre {pair}: {correlation}")
    corr_matrix(survey_df)

    logger.info(count_df.collect())

    input("Press Enter")
    logger.info("Finished HelloSpark")
    # spark.stop()
