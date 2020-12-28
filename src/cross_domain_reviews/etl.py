"""[summary]
    """

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

DIG_MUSIC = 's3a://pyspark3-sample/product_category=Digital_Music_Purchase.parquet/*.snappy.parquet'

DIG_VIDEO = 's3a://pyspark3-sample/product_category=Digital_Video_Download.parquet/*.snappy.parquet'

DIG_GAME = 's3a://pyspark3-sample/product_category=Digital_Video_Games.parquet/*.snappy.parquet'


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

    # music


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
