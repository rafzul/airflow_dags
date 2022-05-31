from airflow.operators.bash import BashOperator
from airflow.models import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
import datetime
import pendulum
# from scripts.parquetizing_data import parquetize_data

#file path, url and type setup

#getting month and year
logical_date = "{{ ds }}"
MONTH = "{{ dag_run.logical_date.strptime('%m') }}"
YEAR = "{{ dag_run.logical_date.strptime('%Y') }}"

#setting up Bash parametrization
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

#setup download data script path
BASH_DATADOWNLOAD="/opt/airflow/dags/repo/scripts/download_data.sh"

#setup download_data path
# FMONTH= `printf "%02d" ${MONTH}`
URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PREFIX="/tmp/nytaxidata/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

#schema file path setup
SCHEMA_FILEPATH="/opt/airflow/dags/repo/schemas/nytaxi_schema_{TAXI_TYPE} "

# #setting up external script path
# EXTSCRIPT_PATH = "/scripts/"

#setting up DAG
default_args = {"owner": "rafzul",
    "start_date": pendulum.datetime(2020, 1, 1, tz="UTC"),
    "end_date": pendulum.datetime(2020, 3, 1, tz="UTC"),
    "depends_on_past": True,
    "retries": 1}


with DAG(
    dag_id="download_dag",
    default_args=default_args,
    schedule_interval="@monthly",  
    catchup=True,
    max_active_runs=2,      
    tags=['nytaxi-dag'],
) as dag:

    # for MONTH in {1..12}: ini didefine di schedule_interval buat jaraknya, trus define start_date dan end_date buat start dan mulenya

    for TAXI_TYPE in {"yellow","green"}:
        with TaskGroup(group_id=f"downloadparquetizegroup_{TAXI_TYPE}") as tg1:
            download_data_task = BashOperator   (
                task_id='download_data',
                bash_command=BASH_DATADOWNLOAD,
                params= {"URL": URL,"LOCAL_PREFIX": LOCAL_PREFIX, "LOCAL_PATH": LOCAL_PATH},        
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


    # download_data_task >> parquetize_data_task




