from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from operators.redshift_tables import CreateRedshiftTablesOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries


#Default parameters for DAG
default_args = {
    'owner': 'udacity',
    'dpends_on_past': False,
    'start_date': datetime(2020, 1, 11),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

#Instantiate DAG
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
         )

#1. Dummy Task - no functionality
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


#2. Copy log files to staging table in Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region='us-west-2',
    json_path="s3://udacity-dend/log_json_path.json"
)

#3. Copy song files to staging table in Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region='us-west-2',
    json_path='auto'
)

#4. Use staging tables to populate fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert,
    delete_first=True 
)

#5. Use staging tables to populate user table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert,
    delete_first=True  
)

#6. Use staging tables to populate song_table table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert,
    delete_first=True     
)

#7. Use staging tables to populate artist table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
    delete_first=True 
)

#8. Use songplay table to populate time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
    delete_first=True     
)

#9. Data check operator to ensure that all tables are populated
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ]    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG dependencies
start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator