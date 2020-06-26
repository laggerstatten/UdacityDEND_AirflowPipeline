from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 insert_sql = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.append_only = append_only

    def execute(self, context):

        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data")
        if not self.append_only:
            delete_sql = "DELETE FROM {}".format(self.table)
            redshift_hook.run(delete_sql)                

        table_insert_sql = "INSERT INTO {table} {insert_sql}".format(table=self.table, insert_sql=self.insert_sql)
        redshift_hook.run(table_insert_sql)