from src.commons import utils
from src.user_item_graph import etl


def run(session, logger, settings):
    """
    get graph edges
    """

    ROOT_PATH_SOURCE = f"{settings['s3a_path']}/cross_domain/{settings['source_reviews_path']}{settings['target_reviews_path']}/"
    ROOT_PATH = f"{settings['s3a_path']}/graph/{settings['source_reviews_path']}{settings['target_reviews_path']}/"

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    source_reviews_path = f"{ROOT_PATH_SOURCE}/{settings['source_reviews_path']}/reviews/*.snappy.parquet"
    target_reviews_path = f"{ROOT_PATH_SOURCE}/{settings['target_reviews_path']}/reviews/*.snappy.parquet"

    source_product_indexed_ids_path = f"{ROOT_PATH_SOURCE}/{settings['source_reviews_path']}/items_idx/*.snappy.parquet"
    target_product_indexed_ids_path = f"{ROOT_PATH_SOURCE}/{settings['target_reviews_path']}/items_idx/*.snappy.parquet"

    # read parquet files from s3
    source_reviews_df = utils.extract_parquet_data(
        session, source_reviews_path)

    source_product_indexed_ids_df = utils.extract_parquet_data(
        session, source_product_indexed_ids_path)

    target_reviews_df = utils.extract_parquet_data(
        session, target_reviews_path)

    target_product_indexed_ids_df = utils.extract_parquet_data(
        session, target_product_indexed_ids_path)

    # transfrom to edges and samples
    source_products_by_customer = etl.to_user_reviewed_products(
        source_reviews_df, source_product_indexed_ids_df)

    source_products_graph = etl.to_items_graph(source_products_by_customer)

    target_products_by_customer = etl.to_user_reviewed_products(
        target_reviews_df, target_product_indexed_ids_df)

    target_products_graph = etl.to_items_graph(target_products_by_customer)

    target_positive_negative_samples = etl.to_user_product_pairs(
        target_products_by_customer,
        target_product_indexed_ids_df,
        'customer_id')

    # +--------------+
    # |         edges|
    # +--------------+
    # |[21757, 25518]|
    # |[45808, 49176]|
    # |[49176, 59357]|
    # |[75557, 73987]|
    # |[83423, 73987]|
    # +--------------+

    utils.load_parquet_to_s3(
        source_products_graph,
        f"{ROOT_PATH}/{settings['source_domain']}/")

    utils.load_parquet_to_s3(
        source_products_graph,
        f"{ROOT_PATH}/{settings['source_domain']}/edges_pairs")

    utils.load_parquet_to_s3(
        target_products_graph,
        f"{ROOT_PATH}/{settings['target_domain']}/edges_pairs")

    # +-----------+--------+--------+
    # |customer_id|positive|negative|
    # +-----------+--------+--------+
    # |   10007421|   21757|    6498|
    # |   10007421|   45808|   30089|
    # |   10007421|   49176|   78406|
    # |   10007421|   21932|   41226|
    # |   10007421|   25518|   60390|
    # +-----------+--------+--------+

    utils.load_parquet_to_s3(
        target_positive_negative_samples,
        f"{settings['s3a_path']}{settings['pn_samples']}{settings['target_domain']}/")

    logger.warn('etl job is finished')
    session.stop()
