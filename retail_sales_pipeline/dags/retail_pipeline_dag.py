from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.abspath('/opt/airflow/scripts'))

from extract_data import extract_data
from transform_data import transform_data
from load_to_db import create_connection, create_table, load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'retail_sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for retail sales data',
    schedule_interval='@daily',
    catchup=False,
)

def extract_task():
    data = extract_data('data\store.csv')
    data.to_csv('/opt/airflow/data/extracted_data.csv', index=False)
    print("Extract complete")

def transform_task():
    import pandas as pd
    data = pd.read_csv('data\store.csv')
    transformed = transform_data(data)
    transformed.to_csv('/opt/airflow/data/transformed_data.csv', index=False)
    print("Transform complete")

def load_task():
    import pandas as pd
    data = pd.read_csv('data\store.csv')
    
    conn = create_connection()
    if conn:
        create_table(conn)
        load_data(conn, data)
        conn.close()
        print("Load complete")

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

extract >> transform >> load