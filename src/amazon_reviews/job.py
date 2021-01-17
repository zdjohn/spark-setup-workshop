from src.commons import utils
from src.amazon_reviews import etl


def run(session, logger, **kwargs):
    """Main ETL script definition.
    :return: None
    """
    # category = kwargs.get('category', '')
    # if not category:
    #     raise Exception('review category not defined')

    settings = utils.load_settings(**kwargs)

    aws_review_s3_raw = settings['raw_path']

    target_s3 = f"{settings['s3a_path']}{settings['category']}"

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    # read parquet files from s3
    aws_review_raw = utils.extract_parquet_data(session, aws_review_s3_raw)

    # get reviewers stats by aggregation
    review_agg_df = etl.to_stats_aggregation(aws_review_raw)

    # get user ids based on user item interaction, i.e. minimal 3 reviews, maximun 50 reviews
    dense_user_ids = etl.to_dense_user_ids(
        review_agg_df, settings['minimum_reviews'], settings['maximum_reviews'])
    # filter reviews records based on dense user_ids
    filtered_review_df = etl.to_dense_reviews(aws_review_raw, dense_user_ids)
    # save data to s3 bucket
    utils.load_parquet_to_s3(filtered_review_df, target_s3)
    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
