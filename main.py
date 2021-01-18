"""[summary]
"""
import argparse
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.commons import utils
import src.amazon_reviews.job as reviews_job
import src.cross_domain_reviews.job as cross_domain


parser = argparse.ArgumentParser()
parser.add_argument("--job", help="job name", required=True)
parser.add_argument("--category", help="review category name")
parser.add_argument("--source_domain", help="source domain category")
parser.add_argument("--target_domain", help="target domain category")
parser.add_argument("--local_run", default=0,
                    help="running spark on local, default False")

JOB_MAPPING = {
    'review': reviews_job.run,
    'cross_domain': cross_domain.run,
}


if __name__ == "__main__":
    load_dotenv()

    args = parser.parse_args()
    spark_session, logger, settings = utils.start_spark(**vars(args))

    if args.local_run:
        utils.s3_credential(spark_session)

    if args.job:
        run_job = JOB_MAPPING.get(args.job)
        run_job(spark_session, logger, settings)
