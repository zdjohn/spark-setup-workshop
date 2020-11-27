# extract data

# transform data

# load data

from pyspark.sql import Row

from etl_project.commons.utils import start_spark


def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log = start_spark()

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    # data = extract_data(spark)
    # load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None
