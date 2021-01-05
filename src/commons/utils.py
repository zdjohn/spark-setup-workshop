import configparser
import os
from os import environ
import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from src.commons.spark_log4j import Log4j

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, 'spark.conf')


def get_spark_app_config():
    spark_conf = SparkConf()
    # todo: replace spark.conf with settings.json
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(CONFIG_PATH)

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def s3_credential(session: SparkSession):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = config.get('default', "aws_access_key_id")
    access_key = config.get('default', "aws_secret_access_key")
    hadoop_conf = session._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)


def start_spark(jar_packages=[], files=[]):
    """[summary]

    Args:
        jar_packages (list, optional): [description]. Defaults to [].
        files (list, optional): [description]. Defaults to [].

    Returns:
        [type]: [description]
    """
    # detect execution environment
    flag_debug = 'DEBUG' in environ.keys()

    spark_builder = SparkSession.builder

    if flag_debug:
        spark_conf = get_spark_app_config()
        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        spark_builder.config(conf=spark_conf)

    # create session and retrieve Spark logger object
    spark_session = spark_builder.getOrCreate()

    if flag_debug:
        s3_credential(spark_session)

    spark_logger = Log4j(spark_session)

    return spark_session, spark_logger


def extract_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    dataframe = spark.read.parquet(path)
    return dataframe


def action_describe(dataframe: DataFrame, columns: list) -> None:
    """table statistics, mean, min, max, count, stddev

    Args:
        df (DataFrame): target dataframe
        columns (list): columns to describe
    """
    df_describe = dataframe.describe(columns)
    df_describe.show()


def load_parquet_to_s3(dataframe: DataFrame, s3_path: str) -> None:
    """save df into s3 in parquet

    Args:
        df (DataFrame): transfromed dataframes
        s3_path (str): target s3 path
    """
    dataframe.write.parquet(f"{s3_path}", mode="overwrite")
