from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime
from pathlib import path
from scripts.parquetizing import parquetize_data

#file path, url and type setup

#setting up Bash parametrization
TAXI_TYPE="yellow"
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

#setup download data script path
bash_datadownload ="../scripts/download_data.sh"

#setup download_data path
# FMONTH= `printf "%02d" ${MONTH}`
URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PREFIX="/media/rafzul/'Terminal Dogma'/nytax idata/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

#schema file path setup
SCHEMA_FILEPATH="/schemas/nytaxi_schema_{TAXI_TYPE}"


#getting month and year
logical_date = "{{ ds }}"
MONTH = datetime.strptime(logical_date, "%m")
YEAR = datetime.strptime(logical_date, "%y")

#setting up external script path
EXTSCRIPT_PATH = "../scripts/"


#setting up DAG
default_args = {"owner": "rafzul",
    "start_date": datetime(2020,1,1),
    "end_date": datetime(2020,2,1),
    "schedule_interval": "@monthly",
    "depends_on_past": False,
    "retries": 1}


with DAG(
    dag_id="download_dag",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,      
    tags=['nytaxi-dag'],
) as dag:

    # for MONTH in {1..12}: ini didefine di schedule_interval buat jaraknya, trus define start_date dan end_date buat start dan mulenya

    # for TAXI_TYPE in {yellow,green}:

    download_data_task = BashOperator(
        task_id='download_data',
        bash_command=bash_datadownload,
        params= {"TAXI_TYPE": TAXI_TYPE, "YEAR": YEAR, "MONTH": MONTH, "URL_PREFIX": URL_PREFIX},        
    )

    with open(SCHEMA_FILEPATH, 'r') as schema_file:
        #read the schema file content
        schema_string = schema_file.read()
        #call the parquetizing function
        parquetize_data_task = PythonOperator(
            task_id="parquetize_data",
            python_callable=parquetize_data,
            op_kwargs={
                "schema": schema_file,
                "csv_file": LOCAL_PATH,
            },
        )


    download_data_task >> parquetize_data_task




