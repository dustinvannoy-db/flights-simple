import pytest


@pytest.fixture(scope="module")
def spark_session():
    try:
        """
        If Databricks connect is installed, it will create a connect session either using ENV variables
        or with a Databricks config profile called "unit_tests"
        
        Note that the python environment cannot contain both Pyspark and Databricks connect at the same time 
        """
        from databricks.connect import DatabricksSession
        try:
            yield DatabricksSession.builder.getOrCreate()
        except (ValueError, RuntimeError, Exception):
            print("Fallback into Databricks Connect config file")
            # https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect/python/install#--a-databricks-configuration-profile
            yield DatabricksSession.builder.profile("unit_tests").getOrCreate()
    except (ModuleNotFoundError, ImportError):
        print("No Databricks Connect, build and return local SparkSession")
        """
        This fixture provides preconfigured SparkSession with Delta support.
        After the test session, temporary warehouse directory is deleted.
        :return: SparkSession
        """
        import logging
        import shutil
        import tempfile
        from pathlib import Path

        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        logging.info("Configuring Spark session for testing environment")
        warehouse_dir = tempfile.TemporaryDirectory().name
        if Path(warehouse_dir).exists():
            shutil.rmtree(warehouse_dir)

        _builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.warehouse.dir", Path(warehouse_dir).as_uri())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.shuffle.partitions", "1")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .enableHiveSupport()
        )
        spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
        logging.info("Spark session configured")
        yield spark
        logging.info("Shutting down Spark session")
        spark.stop()
        if Path(warehouse_dir).exists():
            shutil.rmtree(warehouse_dir)
