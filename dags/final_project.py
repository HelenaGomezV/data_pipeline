from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# Definir argumentos por defecto
default_args = {
    'owner': 'Helena Gomez',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    # Carga de datos desde S3 a Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_creds_id="aws_credentials",
        s3_bucket="helena-datalake",
        s3_key="log-data",
        redshift_table_name="staging_events",
        extra_params="format as json 's3://helena-datalake/log-data/log_json_path.json'",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_creds_id="aws_credentials",
        s3_bucket="helena-datalake",
        s3_key="song-data",
        redshift_table_name="staging_songs",
        extra_params="REGION 'us-west-2' FORMAT as JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL",
    )

    # Carga de la tabla de hechos
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table_name="songplays",
        sql_statement=SqlQueries.songplay_table_insert,
        append_data=False,
    )

    # Carga de las tablas de dimensiones
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table_name="users",
        sql_statement=SqlQueries.user_table_insert,
        append_data=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table_name="songs",
        sql_statement=SqlQueries.song_table_insert,
        append_data=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table_name="artists",
        sql_statement=SqlQueries.artist_table_insert,
        append_data=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table_name="time",
        sql_statement=SqlQueries.time_table_insert,
        append_data=False,
    )

    # Validaciones de calidad de datos
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        data_quality_checks=[
            {'dq_check_sql': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'expected_value': 0},
            {'dq_check_sql': 'SELECT COUNT(*) FROM artists WHERE name IS NULL', 'expected_value': 0},
            {'dq_check_sql': 'SELECT COUNT(*) FROM users WHERE first_name IS NULL', 'expected_value': 0},
            {'dq_check_sql': 'SELECT COUNT(*) FROM time WHERE month IS NULL', 'expected_value': 0},
            {'dq_check_sql': 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL', 'expected_value': 0},
        ],
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    # Definir la secuencia de ejecución del DAG
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
