#!/bin/bash
set -e 

# TAXI_TYPE='$TAXI_TYPE' #taxi_type     
# echo ${TAXI_TYPE}
# MONTH='$MONTH' #month
# echo ${MONTH}
# YEAR='$YEAR' #year
# echo ${YEAR}

TAXI_TYPE='$taxi_type'#taxi_type     
echo ${TAXI_TYPE}
MONTH='$month' #month
echo ${MONTH}
YEAR='$year' #years
echo ${YEAR}

        
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
gzip ${LOCAL_PATH}