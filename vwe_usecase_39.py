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

# snowflake_query = [
#            """ create table wholesale_result as (Select CUSTOMER_ID,CUSTOMER_NAME,COUNTRY,STATES from wholesale where COUNTRY = 'USA' and  STATES = 'California')""",
# ]


with DAG('vwe_usecase_40',
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
        schema='PUBLIC',
        sql = 'call wholesale_result_data()'
    )

    t1 = DummyOperator(task_id='end')

    t0 >> snowflake >> result  >> t1