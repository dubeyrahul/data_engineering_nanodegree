from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_to_run="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_to_run = sql_to_run

    def execute(self, context):
        self.log.info(f'Loading fact table: {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_sql = f"""
            INSERT INTO {self.table}
            {self.sql_to_run}
        """
        redshift.run(rendered_sql)
        self.log.info(f'Loading complete for fact table: {self.table}')
