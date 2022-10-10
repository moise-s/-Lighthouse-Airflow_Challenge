from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from scripts.sqlite import sqlite_to_csv, sqlite_join

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


#  definição da função da TERCEIRA atividade da DAG: criação de arquivo final_output.txt
def export_final_answer():
    import base64

    # Import count
    with open('data/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt", "w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['lighthouse'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    # Instrução para realizar PRIMEIRA atividade da DAG: criação de arquivo CSV
    export_CSV = PythonOperator(
        task_id='export_CSV',
        python_callable=sqlite_to_csv,
        provide_context=True
    )

    # Instrução para realizar SEGUNDA atividade da DAG: criação de arquivo data/count.txt
    join_count = PythonOperator(
        task_id='join_count',
        python_callable=sqlite_join,
        provide_context=True
    )

    # Instrução para realizar TERCEIRA atividade da DAG: criação de arquivo final_output.txt
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

# Ordenação das atividades da DAG
export_CSV >> join_count >> export_final_output
