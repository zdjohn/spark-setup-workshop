"""[summary]

    Returns:
        [type]: [description]
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def to_stats_aggregation(dataframe: DataFrame) -> DataFrame:
    """Transform original dataset.
    :param df: Input DataFrame.

    :return: aggregated dataframes based on users purchase, category, market, etc..
    """
    df_stats_agg = dataframe.groupby('customer_id').agg(
        F.count('product_id').alias('products'),
        F.countDistinct('product_id').alias('products_distinct'),
        F.countDistinct('product_parent').alias('pp_counts'),
        F.count('review_id').alias('reviews'),
        F.count(F.when(F.col('verified_purchase') == 'Y', True)
                ).alias('verified_purchase')
    )

    df_stats_agg = df_stats_agg.withColumn(
        'purchase_rate', (df_stats_agg.verified_purchase/df_stats_agg.reviews).cast("float"))

    return df_stats_agg


def to_dense_user_ids(dataframe: DataFrame, floor: int, ceiling: int) -> DataFrame:
    """filter costomer based on user-item interaction

    Args:
        dataframe (DataFrame): source dataframe with user aggregated metrics
        floor (int): minimal review counts
        ceiling (int): maximun review counts

    Returns:
        DataFrame: users with right density
    """
    filtered_users = dataframe.where(
        (dataframe.products >= floor) & (dataframe.products <= ceiling))
    return filtered_users.select(F.col('customer_id'))


def to_dense_reviews(dataframe: DataFrame, customer_ids: DataFrame) -> DataFrame:
    """filter review df by customer_ids

    Args:
        dataframe (DataFrame): original review df
        user_ids (DataFrame): dataframe with customer_ids only

    Returns:
        DataFrame: dense user review
    """
    return dataframe.join(F.broadcast(customer_ids), ['customer_id'])
