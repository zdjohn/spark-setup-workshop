from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def extract_data(spark: SparkSession, path: str) -> DataFrame:
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.parquet(path)
    return df


def to_stats_aggregation(df: DataFrame) -> DataFrame:
    """Transform original dataset.
    :param df: Input DataFrame.

    :return: Transformed DataFrame.
    """
    df_stats_agg = df.groupby('customer_id').agg(
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


def to_dense_user_ids(df: DataFrame, floor: int, ceiling: int) -> DataFrame:
    filtered_users = df.where(
        (df.products >= floor) & (df.products <= ceiling))
    return filtered_users.select(F.col('customer_id'))


def to_dense_reviews(df: DataFrame, user_ids: DataFrame) -> DataFrame:
    return df.join(F.broadcast(user_ids), ['customer_id'])


def action_describe(df: DataFrame, columns: list) -> DataFrame:
    df_describe = df.describe(columns)
    df_describe.show()


def load_data_to_s3(df: DataFrame, s3_path: str) -> None:
    # df.write.parquet(f"{s3_path}", mode="overwrite")
    print(s3_path)
    df.show(10)
