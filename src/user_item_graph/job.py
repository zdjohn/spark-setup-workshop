
from src.commons import utils
from src.user_item_graph import etl


def run(session, logger, **kwargs):
    """
    get graph edges
    """

    domains_map = {
        'music': 'Digital_Music_Purchase',
        'video': 'Digital_Video_Download',
        'game': 'Digital_Video_Games',
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

    source_reviews = utils.SOURCE_REVIEWS_PATH.format(
        source_domain, target_domain)
    target_reviews = utils.TARGET_REVIEWS_PATH.format(
        source_domain, target_domain)
    customers_indexed_ids = utils.CUSTOMERS_INDEXED_IDS_PATH.format(
        source_domain, target_domain)
    source_product_indexed_ids = utils.SOURCE_PRODUCT_INDEXED_IDS_PATH.format(
        source_domain, target_domain)
    target_product_indexed_ids = utils.TARGET_PRODUCT_INDEXED_IDS_PATH.format(
        source_domain, target_domain)

    # read parquet files from s3
    source_reviews_df = utils.extract_parquet_data(session, source_reviews)
    target_reviews_df = utils.extract_parquet_data(session, target_reviews)
    customers_indexed_ids_df = utils.extract_parquet_data(
        session, customers_indexed_ids)
    source_product_indexed_ids_df = utils.extract_parquet_data(
        session, source_product_indexed_ids)
    target_product_indexed_ids_df = utils.extract_parquet_data(
        session, target_product_indexed_ids)

    source_products_by_customer = etl.to_products_grouped_by_customer(
        source_reviews_df, source_product_indexed_ids_df)
    source_product_edges = etl.to_graph_edges(
        source_products_by_customer, 'products_edges')

    target_products_by_customer = etl.to_products_grouped_by_customer(
        target_reviews_df, target_product_indexed_ids_df)
    target_product_edges = etl.to_graph_edges(
        target_products_by_customer, 'products_edges')

    customers_edges_by_source_domain = etl.to_graph_edges(etl.to_customers_grouped_by_product(
        source_reviews_df, customers_indexed_ids_df), 'customers_edges')
    customers_edges_by_target_domain = etl.to_graph_edges(etl.to_customers_grouped_by_product(
        target_reviews_df, customers_indexed_ids_df), 'customers_edges')

    utils.load_parquet_to_s3(source_product_edges, utils.PRODUCT_EDGES_PATH.format(
        source_domain, target_domain, domain=source_domain))
    utils.load_parquet_to_s3(target_product_edges, utils.PRODUCT_EDGES_PATH.format(
        source_domain, target_domain, domain=target_domain))
    utils.load_parquet_to_s3(customers_edges_by_source_domain, utils.CUSTOMER_EDGES_PATH.format(
        source_domain, target_domain, domain=source_domain))
    utils.load_parquet_to_s3(customers_edges_by_target_domain, utils.CUSTOMER_EDGES_PATH.format(
        source_domain, target_domain, domain=target_domain))
