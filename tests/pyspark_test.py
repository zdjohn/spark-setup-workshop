import pytest
from etl_project.commons.utils import start_spark


@pytest.fixture()
def spark_resource(request):
    session, logger = start_spark()

    def teardown():
        logger.info("tearing down")
        session.stop()

    request.addfinalizer(teardown)

    return session


def test_1_that_needs_resource(spark_resource):
    name = spark_resource.conf.get('spark.app.name')
    print(f'{name} and now I\'m testing things...')
