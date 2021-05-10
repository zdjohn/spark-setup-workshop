from src.commons import utils
from src.amazon_reviews import etl


def run(session, logger, settings):
    """Main ETL script definition.
    :return: None
    """
    # category = kwargs.get('category', '')
    # if not category:
    #     raise Exception('review category not defined')

    aws_review_s3_raw = settings['raw_path']

    target_s3 = f"{settings['s3a_path']}/dense_reviews/{settings['category']}"

    # read parquet files from s3
    aws_review_raw = utils.extract_parquet_data(session, aws_review_s3_raw)

    # get reviewers stats by aggregation
    review_agg_df = etl.to_stats_aggregation(aws_review_raw)

    # get user ids based on user item interaction, i.e. minimal 3 reviews, maximun 50 reviews
    dense_user_product = etl.to_dense_product_user_ids(aws_review_raw,
                                                       review_agg_df,
                                                       settings['minimum_reviews'],
                                                       settings['maximum_reviews'])

    utils.load_parquet_to_s3(dense_user_product, target_s3)
    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
