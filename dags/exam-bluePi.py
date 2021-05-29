import airflow

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'datath',
    'start_date': airflow.utils.dates.days_ago(1),
}


def display_variable():
    my_var = Variable.get("my_var")
    print('Variable: ' + my_var)
    return my_var


with DAG(dag_id='variable_id',
         default_args=default_args,
         schedule_interval="@once") as dag:

    tasks = PythonOperator(
        task_id='display_variable',
        python_callable=display_variable
    )
