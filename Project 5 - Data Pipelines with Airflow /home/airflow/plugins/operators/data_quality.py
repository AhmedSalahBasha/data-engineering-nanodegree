from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 quality_queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_queries = quality_queries

    def execute(self, context):
        self.log.info('DataQualityOperator queries is starting ...!')
        redshift = PostgresHook("redshift")
        
        for query in self.quality_queries:
            result = int(redshift.get_first(sql=query['query'])[0])
            
            if query['check'] == 'greater':
                if result == query['result']:
                    raise AssertionError(f"Check failed: {result} {query['check']} {query['result']}")
            elif query['check'] == 'notnull':
                if result != query['result']:
                    raise AssertionError(f"Check failed: {result} {query['check']} {query['result']}")
        
        self.log.info("All data quality checks have been passed successfully!")
        
        
        
        
        