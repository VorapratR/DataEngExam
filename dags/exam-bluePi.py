import psycopg2
import pandas as pd

from psycopg2 import Error

from airflow.models import DAG
from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from io import BytesIO, StringIO

from airflow.utils.dates import days_ago
from datetime import timedelta


class Config:
    POSTGRESQL_HOST = Variable.get("POSTGRESQL_HOST")
    POSTGRESQL_PORT = Variable.get("POSTGRESQL_PORT")
    POSTGRESQL_USER = Variable.get("POSTGRESQL_USER")
    POSTGRESQL_PASSWORD = Variable.get("POSTGRESQL_PASSWORD")
    POSTGRESQL_DB = Variable.get("POSTGRESQL_DB")

    POSTGRESQL_TABLE = Variable.get("POSTGRESQL_TABLE")
    POSTGRESQL_TABLE_COLUMN_NAME = Variable.get("POSTGRESQL_TABLE_COLUMN_NAME")
    BUCKET_NAME = Variable.get("BUCKET_NAME")
    FOLDER_NAME = Variable.get("FOLDER_NAME")
    PROJECT_ID = Variable.get("PROJECT_ID")

    CREDENTIALS_PATH = Variable.get("CREDENTIALS_PATH")


param_dic = {
    "host": Config.POSTGRESQL_HOST,
    "database": Config.POSTGRESQL_DB,
    "user": Config.POSTGRESQL_USER,
    "password": Config.POSTGRESQL_PASSWORD
}


def connect_db(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df


def control_pass_db_to_datalake():
    # Connect to the database
    conn = connect_db(param_dic)
    table_names = Config.POSTGRESQL_TABLE.split(',')
    table_column_names = Config.POSTGRESQL_TABLE_COLUMN_NAME.split('/')
    print(table_column_names)
    for i, table_name in enumerate(table_names):
        print(table_column_names[i])
        column_names = table_column_names[i].split(',')
        print(column_names)
        df = postgresql_to_dataframe(
            conn, f"select * from {table_name}", column_names)
        save_data_to_dl(table_name, df)


def save_data_to_dl(table_name, retail):
    filename = f"retail_from_{table_name}_table"
    bucket_name = Config.BUCKET_NAME
    retail.to_csv(f"{filename}.csv", index=False)
    hook = GoogleCloudStorageHook()
    hook.upload(bucket_name,
                object='{}.csv'.format(filename),
                filename=f"{filename}.csv",
                mime_type='text/csv')


def return_byte_object(bucket, path: str) -> BytesIO:
    blob = bucket.blob(path)
    byte_object = BytesIO()
    blob.download_to_file(byte_object)
    byte_object.seek(0)
    return byte_object


def control_transform_user_log():
    credentials = service_account.Credentials.from_service_account_file(
        Config.CREDENTIALS_PATH)
    storage_client = storage.Client(
        project=Config.PROJECT_ID, credentials=credentials)
    bucket = storage_client.get_bucket(Config.BUCKET_NAME)

    # create the byte stream from the csv file
    fileobj = return_byte_object(
        bucket, path="retail_from_user_log_table.csv")
    # load the byte stream into panda
    df = pd.read_csv(fileobj)

    print(df['status'])
    df['status'] = df['status'].apply(lambda x: 'True' if x == 1 else 'False')
    print(df['status'])
    # re-upload a different file into google cloud bucket
    bucket.blob('{}.csv'.format('transform_retail_from_user_log_table')).upload_from_string(
        df.to_csv(index=False), 'text/csv')


default_args = {
    'owner': 'VorapratR',
    'start_date': days_ago(1),
    'email': ['voraprat.r@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'schedule_interval': '@once',
}

dag = DAG(
    'exam_bluepi',
    default_args=default_args,
    description='Interview Challenge (Data Engineer) - 4 days',
    schedule_interval=timedelta(days=1),
)

pass_db_to_datalake = PythonOperator(
    task_id='pass_db_to_datalake',
    python_callable=control_pass_db_to_datalake,
    dag=dag,
)

transform_user_log = PythonOperator(
    task_id='transform_user_log',
    python_callable=control_transform_user_log,
    dag=dag,
)

pass_db_to_datalake >> transform_user_log
