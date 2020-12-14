# extract data

# transform data

# load data


import configparser
from os import path

from src.commons import utils
from src.amazon_reviews import job

config = configparser.ConfigParser()
config.read(path.expanduser("~/.aws/credentials"))
access_id = config.get('default', "aws_access_key_id")
access_key = config.get('default', "aws_secret_access_key")

session, logger = utils.start_spark()
hadoop_conf = session._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)


CATEGORY = 'product_category=Digital_Video_Download'
S3_PARQUET_PATH = f's3a://amazon-reviews-pds/parquet/{CATEGORY}/*.snappy.parquet'
TARGET_S3 = f's3a://pyspark3-sample/{CATEGORY}'


def main():
    """Main ETL script definition.
    :return: None
    """
    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    # read parquet files from s3
    dataframe = job.extract_data(session, S3_PARQUET_PATH)
    # get reviewers stats by aggregation
    purchase_agg_df = job.to_stats_aggregation(dataframe)
    # get user ids based on user item interaction, i.e. minimal 3 reviews, maximun 50 reviews
    dense_user_ids = job.to_dense_user_ids(purchase_agg_df, 3, 50)
    # filter reviews records based on dense user_ids
    filtered_review_df = job.to_dense_reviews(dataframe, dense_user_ids)
    # save data to s3 bucket
    job.load_data_to_s3(filtered_review_df, TARGET_S3)
    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
    return None


if __name__ == "__main__":
    main()
