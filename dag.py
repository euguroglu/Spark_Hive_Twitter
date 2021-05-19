from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.sqoop.operators.sqoop import SqoopOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta
import time
import requests
import json

#Define default arguments for dag process
default_args = {
    "owner": "airflow",
    "email_on_failure": True,
    "email_on_retry": True,
    "email": "admin@localhost.com",
    "retry": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("twitter",start_date=datetime(2021, 1, 1),
         schedule_interval="*/10 * * * *", default_args=default_args, catchup=False) as dag:

         #Sqoop Operator
         hive_to_mysql = SqoopOperator(
            task_id = "hive_to_mysql",
            conn_id = "sqoop_conn",
            cmd_type = "export",
            table = "twitter",
            hcatalog_table = "twitter"
         )
