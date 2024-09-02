import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual 
import os
import sys

# Set my library directory to be in the path
currentdir = os.path.dirname(__file__)
parentdir = os.path.dirname(currentdir)
parent_parent_dir =  os.path.dirname(parentdir)
print(parent_parent_dir)
sys.path.insert(0,parent_parent_dir)

from flights.utils import flight_utils

def test_get_flight_schema__valid():
    schema = flight_utils.get_flight_schema()
    assert schema is not None
    assert len(schema) == 31