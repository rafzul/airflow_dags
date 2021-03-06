from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow import macros
from datetime import datetime
from airflow.utils.task_group import TaskGroup
import datetime
import pendulum
# from scripts.parquetizing_data import parquetize_data

#file path, url and type setup

#setting up DAG
default_args = {"owner": "rafzul",
    "start_date": pendulum.datetime(2020, 1, 1, tz="UTC"),
    "end_date": pendulum.datetime(2020, 3, 1, tz="UTC"),
    "depends_on_past": False,
    "retries": 1}


with DAG(
    dag_id="download_dag_test",
    default_args=default_args,
    schedule_interval="@monthly",  
    catchup=True,
    max_active_runs=2,      
    tags=['nytaxi-dag'],
) as dag:

    # for MONTH in {1..12}: ini didefine di schedule_interval buat jaraknya, trus define start_date dan end_date buat start dan mulenya
    taxi_type="green"
    month="{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}"
    year="{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}"

    download_data_task = BashOperator(
                task_id="download_data_test",
                bash_command=f"/scripts/download_data.sh",
                params={'TAXI_TYPE':taxi_type,'MONTH':month,'YEAR':year},      
    )


            # with open(SCHEMA_FILEPATH, 'r') as schema_file:
            #     #read the schema file content
            #     schema_string = schema_file.read()    
            #     #call the parquetizing function
            #     parquetize_data_task = PythonOperator(
            #         task_id="parquetize_data",
            #         python_callable=parquetize_data,
            #         op_kwargs={
            #             "schema": schema_file,
            #             "csv_file": LOCAL_PATH,
            #         },
            #     )
    download_data_task

    for TAXI_TYPE in {"yellow","green"}:
        #setup templating
        #getting month and year
        


    # download_data_task >> parquetize_data_task




