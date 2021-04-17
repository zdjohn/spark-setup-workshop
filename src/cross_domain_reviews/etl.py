"""[summary]
    """

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Tuple


# DIG_MUSIC = 's3a://pyspark3-sample/product_category=Digital_Music_Purchase.parquet/*.snappy.parquet'

# DIG_VIDEO = 's3a://pyspark3-sample/product_category=Digital_Video_Download.parquet/*.snappy.parquet'

# DIG_GAME = 's3a://pyspark3-sample/product_category=Digital_Video_Games.parquet/*.snappy.parquet'


def to_overlapping_customers(source: DataFrame, target: DataFrame) -> DataFrame:
    """[summary]

    Args:
        source (DataFrame): [description]
        target (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    customer_ids = source.join(
        target,
        ['customer_id']
    ).select(F.col('customer_id')).distinct()
    return customer_ids


def to_indexed_ids(df: DataFrame, col_name: str) -> DataFrame:
    """[summary]
    Args:
        df (DataFrame): [description]
        col_name (str): [description]
    Returns:
        DataFrame: [description]
    """
    # ref: https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
    index_window = Window.orderBy(F.col(col_name))
    index_df = df.withColumn(
        f'{col_name}_index', F.row_number().over(index_window)-1)
    return index_df.select(F.col(f'{col_name}_index'), F.col(col_name))


def to_overlapping_reviews(review_df: DataFrame, customer_ids: DataFrame) -> DataFrame:
    """
    Args:
        review_df ([type]): [description]
        DataFrame ([type]): [description]
        user_ids (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return review_df.join(customer_ids, ["customer_id"])

    # video

    # software


def train_test_split(review_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """[summary]

    Args:
        review_df (DataFrame): [description]
        user_ids (DataFrame): [description]
        split (float): [description]

    Returns:
        DataFrame: [description]
    """
    test_user_df = review_df.drop_duplicates('user_id')
    train_user_df = review_df.join(test_user_df, 'left_anti')
    return test_user_df, train_user_df
