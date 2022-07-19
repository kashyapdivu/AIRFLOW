# step - 1 
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy_operator import DummyOperator

# step - 2
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 13),
    'retries' : 0
}

# step - 3
dag = DAG(dag_id = 'DAG-1',
          default_args=default_args,
          catchup = False,
          schedule_interval='@once',
          )

# step - 4
start = DummyOperator(task_id = 'start', dag=dag)
end = DummyOperator(task_id = 'end', dag=dag)

#step - 5
start >> end
