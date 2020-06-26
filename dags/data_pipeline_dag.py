from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator)
from airflow.operators import (LoadFactOperator)
from airflow.operators import (LoadDimensionOperator)
from airflow.operators import (DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag=dag,
    s3_source = "'s3://udacity-dend/log_data'",
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    json_format = "JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag=dag,
    table = 'staging_songs',
    s3_source = "'s3://udacity-dend/song_data'",
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    json_format = "FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    insert_sql = SqlQueries.songplay_table_insert,
    append_only = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    insert_sql = SqlQueries.user_table_insert,
    append_only = False    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    insert_sql = SqlQueries.song_table_insert,
    append_only = False    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    insert_sql = SqlQueries.artist_table_insert,
    append_only = False    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    insert_sql = SqlQueries.time_table_insert,
    append_only = False    
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    #tables = ["songplay", "users", "song", "artist", "time"] 
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},        
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},        
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}       
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table  
load_songplays_table >> load_song_dimension_table  
load_songplays_table >> load_artist_dimension_table  
load_songplays_table >> load_time_dimension_table  

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator