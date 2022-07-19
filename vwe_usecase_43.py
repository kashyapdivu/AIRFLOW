from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import requests


# Default settings applied to all tasks
default_args = {
    'owner': 'divya',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['divya.kashyap@techment.com'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
snowflake_query = [
           """Create table wholesale_result as(
  Select w.CUSTOMER_ID, sum(w."Sales_Amount($)")as Sale_Amount,sum(w.QUANTITY)as Q from wholesale w group by(w.CUSTOMER_ID))""",
]


with DAG('vwe_usecase_43',
         start_date=datetime(2022, 7, 19),
         max_active_runs=3,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:

    t0 = DummyOperator(task_id='start')

    snowflake = S3ToSnowflakeOperator(
        task_id='s3ToSnowflake',
        stage='MY_S3_STAGE/wholesale',
        table='wholesale',
        schema='PUBLIC',
        file_format='File_Format',
        snowflake_conn_id='snowflake_conn'
        )

    result = SnowflakeOperator(
        task_id="vwe_usecase3_result",
        snowflake_conn_id='snowflake_conn',
        role='ACCOUNTADMIN',
        sql = snowflake_query
    )

    t1 = DummyOperator(task_id='end')

    t0 >> snowflake >> result  >> t1