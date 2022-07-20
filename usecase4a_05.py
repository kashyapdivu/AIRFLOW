from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import requests
import logging

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
          """ CREATE TABLE sentiment_final(
                post_id VARCHAR,
                post_text VARCHAR,
                comment_id VARCHAR,
                Source VARCHAR,
                Language VARCHAR,
                comment_text VARCHAR,
                negative VARCHAR,
                neutral VARCHAR,
                positive VARCHAR,
                compound VARCHAR,
                Counts VARCHAR,
                Subsidiary_Name VARCHAR
              )""",
]

result_query=[
    """ Create table sentimental_result as(Select count(distinct SUBSIDIARY_NAME) as total from sentiment_final where SOURCE = 'Facebook')"""
]

with DAG('usecase_4a_05',
         start_date=datetime(2022, 7, 19),
         max_active_runs=3,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:

    t0 = DummyOperator(task_id='start')

    create_table= SnowflakeOperator(
        task_id="create_table",
        snowflake_conn_id='snowflake_conn',
        role='ACCOUNTADMIN',
        sql=snowflake_query
    )

    snowflake = S3ToSnowflakeOperator(
        task_id='s3ToSnowflake',
        stage='MY_S3_STAGE/sentiment_final',
        table='sentiment_final',
        schema='PUBLIC',
        file_format='File_Format',
        snowflake_conn_id='snowflake_conn'
        )

    result_table = SnowflakeOperator(
        task_id="result_table",
        snowflake_conn_id='snowflake_conn',
        role='ACCOUNTADMIN',
        sql=result_query
    )

    t1 = DummyOperator(task_id='end')

    t0 >> create_table >> snowflake >> result_table >> t1