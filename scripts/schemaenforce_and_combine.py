import pyspark
from pyspark.sql import SparkSession, types
from schemas.inferred_schema import green_schema, yellow_schema
import sys


#setting up variables
month = sys.argv[1]
year = sys.argv[2]

#setting files
local_prefix=f"/tmp/nytaxidata/{taxi_type}/{year}/{month}"
local_file=f"{taxi_type}_tripdata_{year}-{month}.parquet"
local_path=f"{local_prefix}/{local_file}"

#setting schema
# if taxi_type = "green":
#     schema = green_schema
# else:
#     schema = yellow_schema
schema = green_schema

#instance a spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('sparknytaxi') \
    .getOrCreate()
    

#enforce standardized schema on file 
def enforce_schema(schema, data_file):
    df_parquetized = spark.read \
    .option("header","true") \
    .schema(schema) \
    .parquet(data_file)

    df_parquetized = df_parquetized.repartition(24)
    df_parquetized.write.parquet("media/rafzul/'Terminal Dogma'/nytaxidata/raw/{TAXI_TYPE}/{YEAR}/{MONTH}")