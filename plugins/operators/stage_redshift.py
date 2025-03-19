from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_creds_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 redshift_table_name: str,
                 extra_params: str = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds_id = aws_creds_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_table_name = redshift_table_name
        self.extra_params = extra_params

    def execute(self, context):
        self.log.info(f'Loading data from S3 to Redshift table {self.redshift_table_name}')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsBaseHook(self.aws_creds_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        copy_sql = f"""
            COPY {self.redshift_table_name}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            {self.extra_params};
        """

        self.log.info(f'Executing COPY command: {copy_sql}')
        redshift.run(copy_sql)
        self.log.info(f'Successfully loaded data into {self.redshift_table_name}')
