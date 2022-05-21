import pyspark
from pyspark.sql import SparkSession, types


schema = schema_file
csv_file = LOCAL_PATH

#instance a spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('sparknytaxi') \
    .getOrCreate()
    

#setting up script for parquetizing
def parquetize_data(schema_file, csv_file):
    df_parquetized = spark.read \
    .option("header", "true") \
    .schema(schema_file) \
    .csv(csv_file)

    df_parquetized = df_parquetized.repartition(24)
    df_parquetized.write.parquet("media/rafzul/'Terminal Dogma'/nytaxidata/raw/{TAXI_TYPE}/{YEAR}/{MONTH}")