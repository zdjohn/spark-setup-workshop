"""[summary]

    Raises:
        Exception: [description]
    """
from src.commons import utils
from src.cross_domain_reviews import etl


def run(session, logger, settings):
    """Main ETL script definition.
    :return: None
    """
    SOURCE_DENSE_REVIEWS = f"{settings['s3a_path']}/dense_reviews/{settings['source_domain']}/*.snappy.parquet"
    TARGET_DENSE_REVIEWS = f"{settings['s3a_path']}/dense_reviews/{settings['target_domain']}/*.snappy.parquet"

    ROOT_PATH = f"{settings['s3a_path']}/cross_domain/{settings['source_domain']}{settings['target_domain']}/"

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    # read parquet files from s3
    source_df = utils.extract_parquet_data(session, SOURCE_DENSE_REVIEWS)
    target_df = utils.extract_parquet_data(session, TARGET_DENSE_REVIEWS)

    # get reviewers stats by aggregation
    refined_source = etl.to_refined_reviews(source_df)
    refined_target = etl.to_refined_reviews(target_df)

    customer_ids_df = etl.to_overlapping_customers(
        refined_source, refined_target)
    customer_ids_df.cache()

    # get user ids based on user item interaction, i.e. minimal 3 reviews, maximun 50 reviews
    source_reviews_df = etl.to_overlapping_reviews(
        refined_source, customer_ids_df)
    # source_reviews_df.cache()

    # filter reviews records based on dense user_ids
    target_reviews_df = etl.to_overlapping_reviews(
        refined_target, customer_ids_df)
    # target_reviews_df.cache()

    # convert customer_id, product_id, into index numbers starting from 0 for training
    indexed_customer_ids = etl.to_indexed_ids(customer_ids_df, 'customer_id')

    indexed_source_product_ids = etl.to_indexed_ids(
        source_reviews_df.select('product_id').distinct(), 'product_id')

    indexed_target_product_ids = etl.to_indexed_ids(
        target_reviews_df.select('product_id').distinct(), 'product_id')

    # todo: get extended users/items from its neighbors (source, target)

    # save data to s3 bucket

    # +-----------+-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+---------------+-----------+-----------+----+
    # |customer_id|marketplace|     review_id|product_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|vine|verified_purchase|review_headline|review_body|review_date|year|
    # +-----------+-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+---------------+-----------+-----------+----+
    # |   10007421|         US|R2NMWIUWWFREBN|B004JCASUM|     496753894|           Hydrology|          4|            0|          0|   N|                Y|     Four Stars| good music| 2014-10-28|2014|
    # |   10007421|         US|R231W2319JSUGI|B0019KF10O|     468285024|      Circles (Edit)|          4|            0|          0|   N|                Y|     Four Stars| good music| 2014-10-28|2014|
    # |   10007421|         US|R3I3K9RNX8BFME|B00545AP9C|     663274082|Tokyo Cries (Glen...|          4|            0|          0|   N|                Y|     Four Stars| good music| 2014-10-28|2014|
    # |   10007421|         US| RN4XRH38NT78D|B0089MZLFK|      73987021|             Contact|          4|            0|          0|   N|                Y|     Four Stars| good music| 2014-10-28|2014|
    # |   10007421|         US|R1MCKHNC6RU3TA|B001A61V60|     468285024|Circles (Original...|          4|            0|          0|   N|                Y|     Four Stars| good music| 2014-10-28|2014|
    # +-----------+-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+---------------+-----------+-----------+----+

    utils.load_parquet_to_s3(
        source_reviews_df,
        f"{ROOT_PATH}/{settings['source_domain']}/reviews")

    utils.load_parquet_to_s3(
        target_reviews_df,
        f"{ROOT_PATH}/{settings['target_domain']}/reviews")

    utils.load_parquet_to_s3(
        indexed_customer_ids,
        f"{ROOT_PATH}/users_idx")

    # +----------------+----------+
    # |product_id_index|product_id|
    # +----------------+----------+
    # |               1|B000Q3CXR4|
    # |               2|B000Q3EL4W|
    # |               3|B000Q3FDT4|
    # |               4|B000Q3JTJO|
    # |               5|B000Q3K812|
    # +----------------+----------+

    utils.load_parquet_to_s3(
        indexed_source_product_ids,
        f"{ROOT_PATH}/{settings['source_domain']}/items_idx")

    utils.load_parquet_to_s3(
        indexed_target_product_ids,
        f"{ROOT_PATH}/{settings['target_domain']}/items_idx")

    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
