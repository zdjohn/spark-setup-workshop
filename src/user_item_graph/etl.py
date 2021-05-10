from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
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


def to_items_graph(products_by_customer_df: DataFrame) -> DataFrame:
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
        DataFrame[node: IntegerType, neighbors: array<IntegerType>]
        e.g.
        +----+--------------------+
        |node|          neighbors|
        +----+--------------------+
        |   0|[18186, 8289, 171...|
        |  10|[10268, 5209, 147...|
        +----+--------------------+
    """
    df = products_by_customer_df.select(
        'customer_id',
        F.explode(udf.pandas_udf_combination('positives')).alias("edges")
    ).select(
        F.split('edges', '-').alias("edges")
    )
    edges_df = df.select(df.edges[0].cast(IntegerType()).alias('node'),
                         df.edges[1].cast(IntegerType()).alias('neighbor')).distinct()

    return edges_df.groupBy('node').agg(
        F.collect_set('neighbor').alias('neighbors'))


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
            |-- positives: array (nullable = true)
            |    |-- element: integer (containsNull = false)
            |-- negatives: array (nullable = true)
            |    |-- element: integer (containsNull = false)
    """

    # shuffle and slice
    products_idx = products_index.select(
        F.collect_set('product_id_index').alias('idx_set'))

    positive_and_neg = products_by_customer_df.crossJoin(
        products_idx
    ).withColumn('all_negatives',
                 F.shuffle(F.array_except(F.col('idx_set'),
                                          F.col('positives')))
                 ).select(
                     F.col(partition_column),
                     F.col('positives'),
                     F.expr("slice(all_negatives, 1, size(positives))").alias(
                         'negatives'))
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
