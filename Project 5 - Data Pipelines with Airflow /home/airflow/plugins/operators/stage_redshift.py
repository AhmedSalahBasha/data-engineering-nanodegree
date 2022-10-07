from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 copy_options='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info(f'StageToRedshiftOperator is started for table: {self.table}')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run("DELETE FROM {};".format(self.table))
        self.log.info("Copying data from S3:{}/{} to Redshift ...".format(self.s3_bucket, self.s3_key))
        
        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_key}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           s3_key=self.s3_key,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           copy_options=self.copy_options)

        redshift.run(copy_query)
        self.log.info("Successfully copied data to Redshift!")





