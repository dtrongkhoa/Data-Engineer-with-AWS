import pendulum
from datetime import timedelta
import os

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

def execute_sql_statements():
    pg_hook = PostgresHook(postgres_conn_id='redshift')
    sql_file_path = os.path.join(os.path.dirname(__file__), 'create_tables.sql')

    with open(sql_file_path, 'r') as file:
        sql_statements = file.read().split(';')

    for statement in sql_statements:
        statement = statement.strip()
        if statement:
            pg_hook.run(statement)

@dag(
    default_args=default_args,
    description='Create tables in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def create_tables():
    start_operator = DummyOperator(task_id='Begin_execution')

    create_redshift_tables = PythonOperator(
        task_id='Create_tables',
        python_callable=execute_sql_statements,
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> create_redshift_tables >> end_operator


create_tables_dag = create_tables()