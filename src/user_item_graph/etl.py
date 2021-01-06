from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from src.user_item_graph import udf


def to_products_grouped_by_customer(dataframe: DataFrame, products_index: DataFrame) -> DataFrame:
    """collect set of product_ids by customer_id
    Args:
        dataframe (DataFrame): [description]

    Returns:
        DataFrame:
        root
            |-- customer_id: string (nullable = true)
            |-- products_edges: array (nullable = true)
            |    |-- element: string (containsNull = true)
    """

    products_by_customer_df = dataframe.join(
        products_index, ['product_id']
    ).groupby('customer_id').agg(
        F.collect_set('product_id_index').alias('products'))

    return products_by_customer_df.select(
        'customer_id',
        udf.pandas_udf_combination('products').alias("products_edges"))


def to_customers_grouped_by_product(dataframe: DataFrame, customers_index: DataFrame) -> DataFrame:
    """collect set of product_ids by customer_id
    Args:
        dataframe (DataFrame): [description]

    Returns:
        DataFrame:
        root
            |-- product_id: string (nullable = true)
            |-- customers_edges: array (nullable = true)
            |    |-- element: string (containsNull = true)
    """
    customers_by_product_df = dataframe.join(
        customers_index, ['customer_id']
    ).groupby('product_id').agg(
        F.collect_set('customer_id_index').alias('customers'))
    return customers_by_product_df.select('product_id',
                                          udf.pandas_udf_combination('customers').alias("customers_edges"))


def to_positive_customer_product_index_pair(dataframe: DataFrame, customers_index: DataFrame, products_index: DataFrame) -> DataFrame:
    """[summary]

    Args:
        dataframe (DataFrame): [description]
        customers_index (DataFrame): [description]
        products_index (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return dataframe.join(customers_index,
                          ['customer_id']).join(products_index,
                                                ['product_id']
                                                ).select('customer_id_index', 'product_id_index')


def to_graph_edges(dataframe: DataFrame, col_name: str) -> DataFrame:
    """
    return unique edges for creating graph
    Args:
        dataframe (DataFrame): [description]
        col_name (str): [description]

    Returns:
        DataFrame:
        root
            |-- edges: string (nullable = true)
    """
    return dataframe.select(F.explode(col_name).alias('edges')).select('edges').distinct()
