from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.user_item_graph import udf


def to_user_reviewed_products(reviews_dataframe: DataFrame, products_index: DataFrame) -> DataFrame:
    """[summary]

    Args:
        reviews_dataframe (DataFrame): [description]
        products_index (DataFrame): [description]

    Returns:
        DataFrame:
        root
            |-- customer_id: string (nullable = true)
            |-- positives: array (nullable = true)
            |    |-- element: integer (containsNull = false)
    """
    return reviews_dataframe.join(
        products_index, ['product_id']
    ).groupby('customer_id').agg(
        F.collect_set('product_id_index').alias('positives'))


def to_edges_by_partition(products_by_customer_df: DataFrame, partition_column: str) -> DataFrame:
    """collect set of product_ids by customer_id
    Args:
        dataframe (DataFrame):
        root
            |-- <partition_column> customer_id: string (nullable = true)
            |-- positives: array (nullable = true)
            |    |-- element: integer (containsNull = false)

        <partition_column> (str): <partition_column> column name
    Returns:
        DataFrame:
        root
            |-- edges: array (nullable = true)
            |    |-- element: string (containsNull = true)
    """
    return products_by_customer_df.select(
        partition_column,
        F.explode(udf.pandas_udf_combination('positives')).alias("edges")
    ).select(
        F.split('edges', '-').alias("edges")).distinct()


# def to_graph_edges(dataframe: DataFrame, partition_column: str) -> DataFrame:
#     """
#     return unique edges for creating graph
#     Args:
#         dataframe (DataFrame):
#          root
#             |-- <partition_column>: string (nullable = true)
#             |-- edges: array (nullable = true)
#             |    |-- element: string (containsNull = true)
#         partition_column (str): [description]

#     Returns:
#         DataFrame:
#         root
#             |-- edges: string (nullable = true)
#     """
#     return dataframe.select(F.explode(partition_column).alias('edges')
#                             ).select(F.split('edges', '-')
#                                      ).distinct()


def to_user_product_pairs(products_by_customer_df: DataFrame, products_index: DataFrame, partition_column: str) -> DataFrame:
    """[summary]

    Args:
        products_by_customer_df (DataFrame):
        root
            |-- <partition_column> customer_id: string (nullable = true)
            |-- positives: array (nullable = true)
            |    |-- element: integer (containsNull = false)
        products_index (DataFrame): [description]

    Returns:
        DataFrame:
        root
            |-- <partition_column> customer_id: string (nullable = true)
            |-- positive: integer (nullable = true)
            |-- negative: integer (nullable = true)
    """
    products_idx = products_index.select(
        F.collect_set('product_id_index').alias('idx_set'))

    positive_and_neg = products_by_customer_df.crossJoin(
        products_idx
    ).withColumn('all_negatives',
                 F.shuffle(F.array_except(F.col('idx_set'),
                                          F.col('positives')))
                 ).select(F.col(partition_column),
                          F.col('positives'),
                          F.expr("slice(all_negatives, 1, size(positives))").alias(
                              'negatives')
                          ).select(partition_column,
                                   F.explode(F.arrays_zip(
                                       'positives', 'negatives')).alias('pair')
                                   ).select(partition_column,
                                            F.col('pair.positives').alias(
                                                'positive'),
                                            F.col('pair.negatives').alias('negative'))

    return positive_and_neg


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


def to_positive_customer_product_index_pair(review_dataframe: DataFrame, customers_index: DataFrame, products_index: DataFrame) -> DataFrame:
    """[summary]

    Args:
        dataframe (DataFrame): [description]
        customers_index (DataFrame): [description]
        products_index (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """

    # get max product index

    return review_dataframe.join(customers_index,
                                 ['customer_id']).join(products_index,
                                                       ['product_id']
                                                       ).select('customer_id_index', 'product_id_index')
