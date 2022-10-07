from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="redshift",
                 sql_query="",
                 mode='append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator is starting for table: {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift.run(f'DELETE FROM {self.table};')
            self.log.info('Deleted data from {self.table} successfully!')
        
        sql = """
            INSERT INTO {}
            {};
        """.format(self.table, self.sql_query)
        
        self.log.info('Loading data into table: {}'.format(self.table))
        redshift.run(sql)
        self.log.info('LoadDimensionOperator is successfully done for table: {}'.format(self.table))
        
