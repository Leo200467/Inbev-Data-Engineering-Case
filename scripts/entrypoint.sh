#!/bin/bash

airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
# "$_AIRFLOW_WWW_USER_USERNAME" -p "$_AIRFLOW_WWW_USER_PASSWORD"

#!/bin/bash
 
apt-get update  
apt-get upgrade -y 

airflow webserver