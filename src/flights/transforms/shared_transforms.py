"""Python functions to test
These represent Python functions that you would keep in a Python file and import to test.
"""
from pyspark.sql.functions import current_timestamp, current_date, expr, lit

def add_metadata_columns(df, include_time=True):
    if include_time:
        df = df.withColumn("last_updated_time", current_timestamp())
    else:
        df = df.withColumn("last_updated_date", current_date())
     
    df = df.withColumn("project", lit("flights_simple"))

    return df
