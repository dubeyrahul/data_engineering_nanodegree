from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    sql_count_template = """
        SELECT COUNT(*) FROM {}
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_to_check=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_to_check = tables_to_check

    def execute(self, context):
        if not self.tables_to_check:
            raise ValueError('Need to pass at least one table to check')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table_to_check in self.tables_to_check:
            self.log.info('Running data quality checks on: {table_to_check}')
            rendered_query = DataQualityOperator.sql_count_template.format(table_to_check)
            records = redshift.get_records(rendered_query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table_to_check} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table_to_check} contained 0 rows")
            self.log.info(f"Data quality on table {table_to_check} check passed with {num_records} records")
