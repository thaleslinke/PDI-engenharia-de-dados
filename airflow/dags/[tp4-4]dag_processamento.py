from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(8),
    'retries': 1,
}

dataset_path = '/tmp/vendas_result.csv'

dag_process = DAG(
    'dag_process',
    default_args=default_args,
    description='DAG de processamento do dataset quando criado',
    schedule=[Dataset(dataset_path)],
)

def ranking():
    input_path = dataset_path
    df = pd.read_csv(input_path)
    
    soma_vendas = df.groupby('funcionario_nome')['preco_de_venda'].sum().reset_index()
    soma_vendas = soma_vendas.sort_values(by='preco_de_venda', ascending=False).reset_index(drop=True)
    soma_vendas['rank'] = soma_vendas.index + 1
    
    for index, row in soma_vendas.iterrows():
        formatted_value = f"R${row['preco_de_venda']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        print(f"Vendedor {row['funcionario_nome']} faturou o valor {formatted_value} e ficou em {row['rank']} lugar")

process_task = PythonOperator(
    task_id='ranking',
    python_callable=ranking,
    dag=dag_process,
)

process_task