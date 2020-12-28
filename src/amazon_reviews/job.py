# extract data

# transform data

# load data


from src.commons import utils
from src.amazon_reviews import etl


def run(session, logger, **kwargs):
    """Main ETL script definition.
    :return: None
    """
    category = kwargs.get('category', '')
    if not category:
        raise Exception('review category not defined')

    s3_parquet_path = f's3a://amazon-reviews-pds/parquet/{category}/*.snappy.parquet'
    target_s3 = f's3a://pyspark3-sample/{category}.parquet'

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    # read parquet files from s3
    dataframe = utils.extract_data(session, s3_parquet_path)
    # get reviewers stats by aggregation
    purchase_agg_df = etl.to_stats_aggregation(dataframe)
    # get user ids based on user item interaction, i.e. minimal 3 reviews, maximun 50 reviews
    dense_user_ids = etl.to_dense_user_ids(purchase_agg_df, 3, 50)
    # filter reviews records based on dense user_ids
    filtered_review_df = etl.to_dense_reviews(dataframe, dense_user_ids)
    # save data to s3 bucket
    utils.action_parquet_to_s3(filtered_review_df, target_s3)
    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
    return None
