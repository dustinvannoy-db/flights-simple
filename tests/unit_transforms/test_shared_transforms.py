import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField
import sys

sys.path.append('./src')

from flights.transforms import shared_transforms

@pytest.fixture(scope="module")
def spark_session():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()      
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


def test_add_metadata__valid(spark_session):
    input_df = spark_session.createDataFrame([
        ["1","test1"],
        ["2","test2"],
        ["3","test3"]
    ], ["id", "name"])

    expected_schema = (
        StructType()
            .add("id", "string")
            .add("name", "string")
            .add("last_updated_date", "date")
            .add("source_project", "string")
    )

    result_df = shared_transforms.add_metadata_columns(input_df, include_time=False)

    assertSchemaEqual(result_df.schema, expected_schema)
