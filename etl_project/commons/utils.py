import __main__

import json
import configparser
import os
from os import environ, listdir, path

from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from .spark_log4j import Log4j

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, 'spark.conf')


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


# get spark session
def start_spark(jar_packages=[], files=[]):
    # detect execution environment
    flag_debug = 'DEBUG' in environ.keys()

    spark_builder = SparkSession.builder

    spark_conf = get_spark_app_config()

    if flag_debug:
        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        spark_builder.config(conf=spark_conf)

    # create session and retrieve Spark logger object
    spark_session = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_session)

    return spark_session, spark_logger
