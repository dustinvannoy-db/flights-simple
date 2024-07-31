from pyspark.sql.functions import col, lit

table = "main.dustinvannoy_dev.flights_raw"

df = spark.read.table(table)

df.select("UniqueCarrier", "DepTime", "FlightNum").limit(100).show()

filtered_df = df.filter(col("UniqueCarrier") == lit("CO")).limit(1000)
filtered_df.select("UniqueCarrier", "DepTime", "FlightNum").show()