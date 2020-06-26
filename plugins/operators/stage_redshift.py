from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_source,
                 table,
                 redshift_conn_id = "",
                 aws_conn_id = "",
                 json_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_source = s3_source
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.json_format = json_format

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
              
        stage_sql = """
                    COPY {table}
                    FROM {s3_source}
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {json_format};
                """.format(table=self.table,
                           s3_source=self.s3_source,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           json_format=self.json_format)
        self.log.info("Staging data to Redshift")
        redshift_hook.run(stage_sql)