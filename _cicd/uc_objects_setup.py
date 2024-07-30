import sys
from pyspark.sql import SparkSession

try:
    catalog = sys.argv[1]
except IndexError:
    catalog = "main"
try:
    flights_schema = sys.argv[2]
except IndexError:
    flights_schema = "dustinvannoy_dev"

# tables = ["flights_raw"]

spark = SparkSession.builder \
    .appName("UC Object Setup") \
    .getOrCreate()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{flights_schema};")