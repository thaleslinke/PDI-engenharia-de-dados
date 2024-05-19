from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'dag_de_conexao',
    default_args=default_args,
    description='DAG conecta ao Postgres para processar dados',
    schedule_interval=None,
)

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
    output_path = '/tmp/vendas_result.csv'
    df.to_csv(output_path, index=False)
    
    cursor.close()
    connection.close()

def ranking():
    input_path = '/tmp/vendas_result.csv'
    df = pd.read_csv(input_path)
    
    soma_vendas = df.groupby('funcionario_nome')['preco_de_venda'].sum().reset_index()
    soma_vendas = soma_vendas.sort_values(by='preco_de_venda', ascending=False).reset_index(drop=True)
    soma_vendas['rank'] = soma_vendas.index + 1
    
    for index, row in soma_vendas.iterrows():
        formatted_value = f"R${row['preco_de_venda']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        print(f"Vendedor {row['funcionario_nome']} faturou o valor {formatted_value} e ficou em {row['rank']}º lugar")
    
query_task = PythonOperator(
    task_id='query_to_csv',
    python_callable=query_to_csv,
    dag=dag,
)

rank_task = PythonOperator(
    task_id='ranking',
    python_callable=ranking,
    dag=dag,
)

query_task >> rank_task