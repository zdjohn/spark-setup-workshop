from pyspark.sql import SparkSession, DataFrame


def extract_data(spark: SparkSession) -> DataFrame:
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .csv('path'))

    return df


def transform_data(df: DataFrame, **kwargs) -> DataFrame:
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    return df
    # df_transformed = (
    #     df
    #     .select(
    #         col('id'),
    #         concat_ws(
    #             ' ',
    #             col('first_name'),
    #             col('second_name')).alias('name'),
    #            (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

    # return df_transformed


def load_data(df: DataFrame):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None
