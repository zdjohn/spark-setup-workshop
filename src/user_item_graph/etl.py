from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def to_products_grouped_by_customer(dataframe: DataFrame, products_index: DataFrame) -> DataFrame:
    """collect set of product_ids by customer_id
    Args:
        dataframe (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """

    return dataframe.join(products_index, ['product_id']).groupby('customer_id').agg(
        F.collect_set('product_id_index').alias('products'))


def to_customers_grouped_by_product(dataframe: DataFrame) -> DataFrame:
    """collect set of product_ids by customer_id
    Args:
        dataframe (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return dataframe.groupby('product_id').agg(
        F.collect_set('customer_id').alias('customers'))
