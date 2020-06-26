from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('table_create_dag',
          default_args=default_args,
          description='Create tables to receive data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#this task runs the SQL queries in create_tables.sql to create initial tables at start of process
create_tables_task = PostgresOperator(
  task_id = 'create_tables',
  dag = dag,
  sql = 'create_tables.sql',
  postgres_conn_id = 'redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> end_operator
