from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_query = DAG(
    'dag_query',
    default_args=default_args,
    description='DAG para consulta Postgres e criação de csv',
    schedule_interval='@daily',
)

dataset_path = '/tmp/vendas_result.csv'

def query_to_csv():
    query = """
    SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
    FROM vendas v
    JOIN carros c ON v.carro_id = c.id
    JOIN clientes cl ON v.cliente_id = cl.id
    JOIN funcionarios f ON v.funcionario_id = f.id;
    """
    
    postgres_hook = PostgresHook(postgres_conn_id='conexao_teste')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    
    df = pd.DataFrame(results, columns=column_names)
    output_path = dataset_path
    df.to_csv(output_path, index=False)
    
    cursor.close()
    connection.close()

query_task = PythonOperator(
    task_id='query_to_csv',
    python_callable=query_to_csv,
    dag=dag_query,
    outlets=[Dataset(dataset_path)],
)

query_task