#!/bin/bash
#setting up bash script for downloading data (minus the looping, will be done together with the parquetization task for each taxi type/month/   x`year inside dag)

set -e 

TAXI_TYPE=$1 #taxi_type     
MONTH=$2 #month     
YEAR=$3 #year


#setting up Bash parametrization
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.csv"
LOCAL_PREFIX="/tmp/nytaxidata/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

echo "downloading ${URL} to ${LOCAL_PATH}"
mkdir -p ${LOCAL_PREFIX}
wget ${URL} -O ${LOCAL_PATH}

echo "compressing ${LOCAL_PATH}"
gzip