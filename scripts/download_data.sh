#!/bin/bash
#setting up bash script for downloading data (minus the looping, will be done together with the parquetization task for each taxi type/month/   x`year inside dag)

set -e 



echo "downloading {{ params.URL }} to {{ params.LOCAL_PATH }}"
mkdir -p "{{ params.LOCAL_PREFIX }}"
wget "{{ params.URL }} -O {{ params.LOCAL_PATH }}"

echo "compressing {{ params.LOCAL_PATH }}"
gzip