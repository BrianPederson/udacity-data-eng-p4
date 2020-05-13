#!/bin/bash

echo Attempting to kill Airflow process...

airflow_pid=$( ps aux | grep -F "gunicorn: master" | grep -Fv "grep" | awk '{print $2}' )
echo $airflow_pid

kill $airflow_pid
