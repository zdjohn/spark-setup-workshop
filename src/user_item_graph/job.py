from src.commons import utils
from src.user_item_graph import etl


def run(session, logger, settings):
    """
    get graph edges
    """

    # log that main ETL job is starting
    logger.warn('etl_job is up-and-running')
    # execute ETL pipeline

    source_reviews_path = f"{settings['s3a_path']}{settings['source_reviews_path']}"
    target_reviews_path = f"{settings['s3a_path']}{settings['target_reviews_path']}"

    source_product_indexed_ids_path = f"{settings['s3a_path']}{settings['source_product_indexed_ids_path']}"
    target_product_indexed_ids_path = f"{settings['s3a_path']}{settings['target_product_indexed_ids_path']}"

    # read parquet files from s3
    source_reviews_df = utils.extract_parquet_data(
        session, source_reviews_path)
    source_product_indexed_ids_df = utils.extract_parquet_data(
        session, source_product_indexed_ids_path)

    target_reviews_df = utils.extract_parquet_data(
        session, target_reviews_path)
    target_product_indexed_ids_df = utils.extract_parquet_data(
        session, target_product_indexed_ids_path)

    # customers_indexed_ids_df = utils.extract_parquet_data(
    #     session, customers_indexed_ids_path)

    # transfrom to edges and samples
    source_products_by_customer = etl.to_user_reviewed_products(
        source_reviews_df, source_product_indexed_ids_df)

    source_products_edges = etl.to_edges_by_partition(
        source_products_by_customer,
        'customer_id')

    source_positive_negative_samples = etl.to_user_product_pairs(
        source_products_by_customer, source_product_indexed_ids_df, 'customer_id')

    target_products_by_customer = etl.to_user_reviewed_products(
        target_reviews_df, target_product_indexed_ids_df)

    target_products_edges = etl.to_edges_by_partition(
        target_products_by_customer,
        'customer_id')

    target_positive_negative_samples = etl.to_user_product_pairs(
        target_products_by_customer, target_product_indexed_ids_df, 'customer_id')

    # target_products_by_customer = etl.to_user_reviewed_products(
    #     target_reviews_df, target_product_indexed_ids_df)
    # target_product_edges = etl.to_graph_edges(
    #     target_products_by_customer, 'products_edges')

    # customers_edges_by_source_domain = etl.to_graph_edges(etl.to_customers_grouped_by_product(
    #     source_reviews_df, customers_indexed_ids_df), 'customers_edges')
    # customers_edges_by_target_domain = etl.to_graph_edges(etl.to_customers_grouped_by_product(
    #     target_reviews_df, customers_indexed_ids_df), 'customers_edges')

    utils.load_parquet_to_s3(
        source_products_edges,
        f"{settings['s3a_path']}{settings['product_edges']}{settings['source_domain']}/")

    utils.load_parquet_to_s3(
        source_positive_negative_samples,
        f"{settings['s3a_path']}{settings['pn_samples']}{settings['source_domain']}/")

    utils.load_parquet_to_s3(
        target_products_edges,
        f"{settings['s3a_path']}{settings['product_edges']}{settings['target_domain']}/")

    utils.load_parquet_to_s3(
        target_positive_negative_samples,
        f"{settings['s3a_path']}{settings['pn_samples']}{settings['target_domain']}/")

    logger.warn('etl job is finished')
    session.stop()
    # utils.load_parquet_to_s3(target_product_edges,
    #                          settings['target_product_edges'])
    # utils.load_parquet_to_s3(
    #     customers_edges_by_source_domain, settings['customers_edges_by_source_domain'])
    # utils.load_parquet_to_s3(
    #     customers_edges_by_target_domain, settings['customers_edges_by_target_domain'])
