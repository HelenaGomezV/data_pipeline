from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables  # Lista de tablas a verificar
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running Data Quality Checks")

        # Verificar si las tablas tienen datos
        for table in self.tables:
            self.log.info(f"Checking table {table} for data presence.")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if not records or not records[0] or records[0][0] == 0:
                raise ValueError(f"Data quality check failed: {table} is empty!")

            self.log.info(f"Table {table} passed the data presence check with {records[0][0]} records.")

        # Ejecutar validaciones personalizadas
        count_errors = 0
        for check in self.data_quality_checks:
            dq_query = check.get('dq_check_sql')
            expected_value = check.get('expected_value')

            self.log.info(f"Executing DQ query: {dq_query}")
            result = redshift_hook.get_records(dq_query)

            if not result or not result[0]:
                raise ValueError(f"Data quality check failed: Query returned no results -> {dq_query}")

            actual_value = result[0][0]
            if actual_value != expected_value:
                self.log.error(f"Data quality check failed: {dq_query} expected {expected_value} but got {actual_value}")
                count_errors += 1

        if count_errors > 0:
            raise ValueError("One or more Data Quality checks failed!")

        self.log.info("All Data Quality checks passed successfully.")
