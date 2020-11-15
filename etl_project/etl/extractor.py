
def extract_data(spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .parquet('tests/test_data/employees'))

    return df
