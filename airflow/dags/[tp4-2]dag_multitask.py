from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'dag_do_exercício_2',
    default_args=default_args,
    description='dag para exercício de múltiplas tasks e XCOM',
    schedule_interval=timedelta(minutes=30),
)

task_inicio = BashOperator(
    task_id='bash_print',
    bash_command='echo "DAG iniciada"',
    dag=dag,
)

def envio_ds(**kwargs):
    ds = kwargs['ds']
    print(f"{ds}")
    return ds

task_ds = PythonOperator(
    task_id='envio_ds',
    python_callable=envio_ds,
    provide_context=True,
    dag=dag,
)

def envio_ts(**kwargs):
    ts = kwargs['ts']
    print(f"{ts}")
    return ts

task_ts = PythonOperator(
    task_id='envio_ts',
    python_callable=envio_ts,
    provide_context=True,
    dag=dag,
)

task_dummy = DummyOperator(
    task_id='sincronizador',
    dag=dag,
)

def ds_ts(**kwargs):
    ti = kwargs['ti']
    ds = ti.xcom_pull(task_ids='envio_ds')
    ts = ti.xcom_pull(task_ids='envio_ts')
    print(f"{ds} -- {ts}")

task_ds_ts = PythonOperator(
    task_id='ds_ts',
    python_callable=ds_ts,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
task_inicio >> [task_ds, task_ts] >> task_dummy >> task_ds_ts
