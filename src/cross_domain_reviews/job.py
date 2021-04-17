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

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    # read parquet files from s3
    source_df = utils.extract_parquet_data(
        session, f"{settings['s3a_path']}{settings['source_domain']}/*.snappy.parquet")
    target_df = utils.extract_parquet_data(
        session, f"{settings['s3a_path']}{settings['target_domain']}/*.snappy.parquet")

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
        f"{settings['s3a_path']}{settings['source_reviews_path']}")
    utils.load_parquet_to_s3(
        target_reviews_df,
        f"{settings['s3a_path']}{settings['target_reviews_path']}")

    # +-----------------+-----------+
    # |customer_id_index|customer_id|
    # +-----------------+-----------+
    # |                1|   10001105|
    # |                2|   10007421|
    # |                3|   10008274|
    # |                4|   10010722|
    # |                5|   10012171|
    # +-----------------+-----------+

    utils.load_parquet_to_s3(
        indexed_customer_ids,
        f"{settings['s3a_path']}{settings['customers_indexed_ids_path']}")

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
        f"{settings['s3a_path']}{settings['source_product_indexed_ids_path']}")
    utils.load_parquet_to_s3(
        indexed_target_product_ids,
        f"{settings['s3a_path']}{settings['target_product_indexed_ids_path']}")

    # log the success and terminate Spark application
    logger.warn('etl job is finished')
    session.stop()
