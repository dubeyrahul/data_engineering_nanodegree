from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    staging_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_config="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.aws_credentials = aws_credentials
        self.json_config=json_config
        self.log.info("Initialized StageToRedshiftOperator:")
        self.log.info(self.table, self.aws_credentials, self.s3_bucket)


    def execute(self, context):
        # Get AWS Hook
        aws_hook = AwsHook(self.aws_credentials)
        self.log.info(aws_hook)
        # Get credentials from aws hook
        credentials = aws_hook.get_credentials()
        # Create redshift connection using PostgresHook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Remove data from destination table if it exists
        self.log.info(f'Clearing data from {self.table}')
        redshift.run(f'DELETE FROM {self.table}')

        # Now copy data from S3 to Redshift
        self.log.info(f'Copying from s3://{self.s3_bucket}/{self.s3_key} to {self.table}')
        # Use context variable to render S3 key (e.g. past dates)
        rendered_s3_key = self.s3_key.format(**context)
        rendered_s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'
        rendered_sql = StageToRedshiftOperator.staging_sql.format(
            self.table,
            rendered_s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_config,
        )
        redshift.run(rendered_sql)
