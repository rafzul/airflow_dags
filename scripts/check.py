from airflow import macros 
MONTH = {{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}

URL=f"a_tripdata_{MONTH}"

print(URL)