from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag = DAG(
    'dag_do_exercicio_1',
    description='teste de dag para o exercício n. 1',
    schedule_interval=timedelta(minutes=3),
    start_date=datetime(2024,5,16),
)

def funcao_teste():
    print(datetime.now())

task_teste = PythonOperator(
    task_id='test_test',
    python_callable=funcao_teste,
    dag=dag,
    )
