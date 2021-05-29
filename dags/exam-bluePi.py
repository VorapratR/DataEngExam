import airflow
import psycopg2

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta


class Config:

    POSTGRESQL_HOST = Variable.get("POSTGRESQL_HOST")
    POSTGRESQL_PORT = Variable.get("POSTGRESQL_PORT")
    POSTGRESQL_USER = Variable.get("POSTGRESQL_USER")
    POSTGRESQL_PASSWORD = Variable.get("POSTGRESQL_PASSWORD")
    POSTGRESQL_DB = Variable.get("POSTGRESQL_DB")


def get_data_from_db():

    # Connect to the database
    connection = psycopg2.connect(user=Config.POSTGRESQL_USER,
                                  password=Config.POSTGRESQL_PASSWORD,
                                  host=Config.POSTGRESQL_HOST,
                                  port=Config.POSTGRESQL_PORT,
                                  database=Config.POSTGRESQL_DB)

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    return connection.get_dsn_parameters()


# def display_variable():
#     my_var = Variable.get("my_var")
#     print('Variable: ' + my_var)
#     return my_var

default_args = {
    'owner': 'VorapratR',
    'start_date': days_ago(1),
}

dag = DAG(
    'exam_bluepi',
    default_args=default_args,
    description='Interview Challenge (Data Engineer) - 4 days',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='get_data_from_db',
    python_callable=get_data_from_db,
    dag=dag,
)

t1
