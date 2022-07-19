import pyspark 
from pyspark.sql import SparkSession
import pandas as pd


spark = SparkSession.builder \
    .appName('test_schemast') \
    .getOrCreate()
    
df = spark.read \
    .option("header", "true") \
    .parquet("/winpop/k3s_storage/pvc-c8863ef9-f04b-4c11-94ff-740a6deb7e0b_airflow_worker-tmp/nytaxidata/green/2020/01/green_tripdata_2020-01.parquet")
    
print(df.schema)