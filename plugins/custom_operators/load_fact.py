from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        self.log.info(f'Starting to load fact table {self.table_name}')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info(f'Truncating table {self.table_name}')
            redshift.run(f"TRUNCATE TABLE {self.table_name}")

        self.log.info(f'Inserting data into {self.table_name}')
        redshift.run(self.sql_statement)

        self.log.info(f'Finished loading fact table {self.table_name}')
