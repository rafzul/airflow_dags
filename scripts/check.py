from airflow import macros 
MONTH = {{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}

URL=f"a_tripdata_{MONTH}"

print(URL)

# HDFS Tasks
create_hdfs_geolocation_ipv4_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_geolocation_ipv4_dir',
    directory='/user/hadoop/geolocation/raw/ipv4/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag
)