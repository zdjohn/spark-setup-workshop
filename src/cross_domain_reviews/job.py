"""[summary]

    Raises:
        Exception: [description]
    """
from src.commons import utils
from src.cross_domain_reviews import etl


def run(session, logger, **kwargs):
    """Main ETL script definition.
    :return: None
    """
    domains_map = {
        'music': etl.DIG_MUSIC,
        'video': etl.DIG_VIDEO,
        'game': etl.DIG_GAME,
    }

    source_domain = kwargs.get('source_domain', '')
    if not domains_map.get(source_domain):
        raise Exception('source_domain category not defined')

    target_domain = kwargs.get('target_domain', '')
    if not domains_map.get(target_domain):
        raise Exception('target_domain category not defined')

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    # read parquet files from s3
    source_df = utils.extract_parquet_data(
        session, domains_map.get(source_domain))
    target_df = utils.extract_parquet_data(
        session, domains_map.get(target_domain))
    # get reviewers stats by aggregation
    customer_ids_df = etl.to_overlapping_customers(source_df, target_df)
    customer_ids_df.cache()
    # get user ids based on user item interaction, i.e. minimal 3 reviews, maximun 50 reviews
    source_reviews_df = etl.to_overlapping_reviews(source_df, customer_ids_df)
    source_reviews_df.cache()
    # filter reviews records based on dense user_ids
    target_reviews_df = etl.to_overlapping_reviews(target_df, customer_ids_df)
    target_reviews_df.cache()

    # convert customer_id, product_id, into index numbers starting from 0 for training
    indexed_customer_ids = etl.to_indexed_ids(customer_ids_df, 'customer_id')

    indexed_source_product_ids = etl.to_indexed_ids(
        source_reviews_df.select('product_id').distinct(), 'product_id')

    indexed_target_product_ids = etl.to_indexed_ids(
        target_reviews_df.select('product_id').distinct(), 'product_id')

    # save data to s3 bucket
    utils.load_parquet_to_s3(
        indexed_customer_ids, f's3a://pyspark3-sample/{source_domain}_{target_domain}_customer_ids')
    utils.load_parquet_to_s3(
        indexed_source_product_ids, f's3a://pyspark3-sample/{source_domain}{target_domain}_product_ids')
    utils.load_parquet_to_s3(
        indexed_target_product_ids, f's3a://pyspark3-sample/{target_domain}{source_domain}_product_ids')
    utils.load_parquet_to_s3(
        source_reviews_df, f's3a://pyspark3-sample/{source_domain}{target_domain}_reviews')
    utils.load_parquet_to_s3(
        target_reviews_df, f's3a://pyspark3-sample/{target_domain}{source_domain}_reviews')

    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
