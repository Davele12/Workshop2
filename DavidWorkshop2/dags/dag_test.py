from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import pandas as pd

def make_db_engine():
    return create_engine('postgresql://airflow:airflow@host.docker.internal/airflow')

def query_data(sql_query):
    engine = make_db_engine()
    with engine.connect() as connection:
        return pd.read_sql(sql_query, con=connection.connection)

def transform_data(df):
    df['published_at'] = pd.to_datetime(df['published_at'])
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    df = df.drop(columns=['updated_at', 'img'])
    return df

def etl_process(sql_query):
    df = query_data(sql_query) 
    transformed_df = transform_data(df)
    return transformed_df

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Define el DAG
dag = DAG(
    'ETL_DAG',
    default_args=default_args,
    description='ETL_DAG',
    schedule_interval=timedelta(days=1),
)

# Define las tareas utilizando PythonOperator
read_db_task = PythonOperator(
    task_id='read_db',
    python_callable=query_data,
    op_kwargs={'sql_query': 'SELECT * FROM "grammyAwards_Airflow" LIMIT 5;'},
    dag=dag,
)
transform_db_task = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process,
    op_kwargs={'sql_query': 'SELECT * FROM "grammyAwards_Airflow"'},
    dag=dag,
)

# Define la secuencia de tareas
read_db_task >> transform_db_task

