import pendulum


from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from udacity.common import sql_statements2

@dag(
    start_date=pendulum.now()
)
def data_lineage():


    @task()
    def load_trip_data_to_redshift():
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements2.COPY_ALL_TRIPS_SQL.format(
            aws_connection.login,
            aws_connection.password,
        )
        redshift_hook.run(sql_stmt)

    load_trip_data_to_redshift_task= load_trip_data_to_redshift()

    @task()
    def load_station_data_to_redshift():
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements2.COPY_STATIONS_SQL.format(
            aws_connection.login,
            aws_connection.password,
        )
        redshift_hook.run(sql_stmt)

    load_station_data_to_redshift_task = load_station_data_to_redshift()

    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements2.CREATE_TRIPS_TABLE_SQL
    )


    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements2.CREATE_STATIONS_TABLE_SQL,
    )

    location_traffic_drop = PostgresOperator(
        task_id="location_traffic_drop",
        postgres_conn_id="redshift",
        sql=sql_statements2.LOCATION_TRAFFIC_SQL_DROP,
        trigger_rule="all_done"
    )

    location_traffic_create = PostgresOperator(
        task_id="location_traffic_create",
        postgres_conn_id="redshift",
        sql=sql_statements2.LOCATION_TRAFFIC_SQL_CREATE
    )

    create_trips_table >> load_trip_data_to_redshift_task >> location_traffic_drop >> location_traffic_create
    create_stations_table >> load_station_data_to_redshift_task 
data_lineage_dag = data_lineage()