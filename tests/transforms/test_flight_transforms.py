import os
import sys

import pandas as pd
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
import dbldatagen as dg
import pyspark.sql.types as T
from data.delay_type import delay_type_usecases

# Set my library directory to be in the path
currentdir = os.path.dirname(__file__)
parentdir = os.path.dirname(currentdir)
parent_parent_dir =  os.path.dirname(parentdir)
print(parent_parent_dir)
sys.path.insert(0,parent_parent_dir)

from flights.transforms import flight_transforms
from flights.utils import flight_utils


def test_get_flight_schema__valid():
    schema = flight_utils.get_flight_schema()
    assert schema is not None
    assert len(schema) == 31


def test_delay_type_transform__valid(spark_session):
    expected_dataset_pd = pd.DataFrame(delay_type_usecases)
    expected_spec = (
        dg.DataGenerator(spark_session, name="expected", rows=len(delay_type_usecases))
        .withIdOutput()
    )
    for col in expected_dataset_pd.columns:
        expected_spec = expected_spec.withColumn(col, T.StringType(), values=expected_dataset_pd[col])
    data_df = expected_spec.build()

    # data_df = spark_session.createDataFrame(delay_type_usecases)
    input_df = data_df.drop("delay_type")
    expected_df = data_df.select("delay_type")

    # expected_df = spark_session.createDataFrame(expected_data,["delay_type"])

    result_df = flight_transforms.delay_type_transform(input_df)

    assertDataFrameEqual(result_df.select("delay_type"), expected_df)
    

