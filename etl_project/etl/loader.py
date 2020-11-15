

def load_data(df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None
