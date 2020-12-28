"""[summary]
"""
import argparse
import configparser
from os import path
from pyspark.sql import SparkSession

from src.commons import utils
import src.amazon_reviews.job as reviews_job

parser = argparse.ArgumentParser()
parser.add_argument("--job", help="job name", required=True)
parser.add_argument("--category", help="review category name")

JOB_MAPPING = {
    'review': reviews_job.run
}


def _s3_credential(session: SparkSession):
    config = configparser.ConfigParser()
    config.read(path.expanduser("~/.aws/credentials"))
    access_id = config.get('default', "aws_access_key_id")
    access_key = config.get('default', "aws_secret_access_key")
    hadoop_conf = session._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)


if __name__ == "__main__":
    spark_session, logger = utils.start_spark()
    _s3_credential(spark_session)

    args = parser.parse_args()
    if args.job:
        run_job = JOB_MAPPING.get(args.job)
        run_job(spark_session, logger, **vars(args))
