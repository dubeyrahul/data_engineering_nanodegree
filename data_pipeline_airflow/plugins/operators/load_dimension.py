from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_to_run="",
                 load_mode="truncate_insert",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_to_run = sql_to_run
        self.load_mode = load_mode
    def execute(self, context):
        self.log.info(f'Loading dimension table: {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.load_mode not in ['truncate_insert', 'append']:
            raise ValueError('load_mode can only be truncate_insert or append')
        if self.load_mode == 'truncate_insert':
            self.log.info('Loading dimension table: {self.table} with truncate_insert mode')
            truncate_sql = f'DELETE FROM {self.table}'
            self.log.info('Deleting {self.table}')
            redshift.run(truncate_sql)

        self.log.info(f'Inserting into {self.table}')
        insert_sql = f'INSERT INTO {self.table} {self.sql_to_run}'
        redshift.run(insert_sql)
        self.log.info(f'Loading complete for dimension table: {self.table}')
