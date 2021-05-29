import airflow
import psycopg2
from psycopg2 import Error

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


def control_connect_db():
    tables = ["users", "user_log"]

    for table in tables:
        get_data_from_db(table)


def get_data_from_db(table_name):
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user=Config.POSTGRESQL_USER,
                                      password=Config.POSTGRESQL_PASSWORD,
                                      host=Config.POSTGRESQL_HOST,
                                      port=Config.POSTGRESQL_PORT,
                                      database=Config.POSTGRESQL_DB)
        cursor = connection.cursor()
        postgreSQL_select_Query = f"select * from {table_name}"

        cursor.execute(postgreSQL_select_Query)
        result_records = cursor.fetchall()
        print(table_name)
        save_data_to_datalake(result_records)
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def save_data_to_datalake(result_records):
    for row in result_records:
        print("Row = ", row)


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
    task_id='control_connect_db',
    python_callable=control_connect_db,
    dag=dag,
)

t1
