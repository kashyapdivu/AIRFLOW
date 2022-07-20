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
          """ CREATE OR REPLACE TABLE multi_attribute(
Advertiser VARCHAR,
Campaign_Type VARCHAR,
Insertion_Order VARCHAR,
Channel VARCHAR,
Month VARCHAR,
Year VARCHAR,
Unique_Monthly_Visitors INT,
"Cost($)" INT,
"LTV($)" INT,
"CPA/CAC($)" INT,
"COGS($)" INT,
"ROI(%)" INT,
"ROAS(%)" INT,
Likes INT,
Impressions INT,
Clicks INT,
"Click_Rate(CTR)(%)" INT,
"Open_Rate(%)" INT,
"CPM($)" INT,
"CPC($)" INT,
"CPA($)" INT,
Total_Ad_Spend Float,
Post_Click_Conversions INT,
Post_View_Conversions INT,
Total_Conversions INT,
"Revenue(AdvertiserCurrency)($)" INT,
"Revenue_eCPM(AdvertiserCurrency)($)" INT,
"Total_Revenue($)" INT,
"Profit($)" INT,
"Revenue_Growth(%)" INT,
Lead_Generation INT,
AOV INT,
Total_Leads INT,
"Depletion_Volume(%)" INT,
"CPL($)" INT,
LTCR INT,
"CAC($)" INT,
Campaign_ID INT,
Insertion_Order_ID VARCHAR,
Start_date VARCHAR,
End_date VARCHAR,
Increased_Tasting_Room_Counts INT,
Tasting_Room_Location VARCHAR
);
""",

]

with DAG('usecase_4b_03',
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
        stage='MY_S3_STAGE/multi_attribute',
        table='multi_attribute',
        schema='PUBLIC',
        file_format='File_Format',
        snowflake_conn_id='snowflake_conn'
        )


    result_table = SnowflakeOperator(
        task_id="result_table",
        snowflake_conn_id='snowflake_conn',
        role='ACCOUNTADMIN',
        sql=""" Create table multi_attribute_res as(Select CHANNEL,count(IMPRESSIONS)as impressions, count(CLICKS) as clicks from multi_attribute
 group by (CHANNEL)); """
    )

    t1 = DummyOperator(task_id='end')

    t0 >> create_table >> snowflake >> result_table>> t1