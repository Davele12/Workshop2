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

def read_csv():
    df_csv = pd.read_csv('/opt/airflow/dags/files/spotify_dataset.csv')
    if df_csv is not None:
        return df_csv
    else:
        raise ValueError("CSV data could not be loaded.")

def transform_csv():
    df = pd.read_csv('/opt/airflow/dags/files/spotify_dataset.csv')
    df.drop('Unnamed: 0', axis=1, inplace=True)
 

def merge_dataframes(df1, df2, key1, key2):
    merged_df = pd.merge(df1, df2, left_on=key1, right_on=key2, how='inner')
    
    return merged_df

def merge_task(**kwargs):
    # Obtiene los DataFrames de XCom
    df_db = kwargs['ti'].xcom_pull(task_ids='read_db_task')
    df_csv = kwargs['ti'].xcom_pull(task_ids='read_csv_task')
    
    # Verifica que ambos DataFrames no sean None
    if df_db is None or df_csv is None:
        raise ValueError("One or more input DataFrames are None.")
    
    # Continúa con el proceso de merge si ambos DataFrames están presentes
    merged_df = merge_dataframes(df_csv, df_db, 'artists', 'artist')
    # ...
    return merged_df



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

dag = DAG(
    'ETL_DAG',
    default_args=default_args,
    description='ETL_DAG',
    schedule_interval=timedelta(days=1),
)

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

read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)

transform_csv_task = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_csv,
    dag=dag,
)

merge = PythonOperator(
    task_id='merge_data',
    python_callable=merge_task,
    dag=dag,
)

#load_task = PythonOperator(
#    task_id='load_data',
#    python_callable=load_data,  # Asumiendo que tienes una función definida para esto
#    op_kwargs={'table_name': 'merged_data'},  # O los argumentos apropiados
#    dag=dag,
#)

read_csv_task >> transform_csv_task
read_db_task >> transform_db_task

# La tarea de merge debería ejecutarse después de ambas transformaciones
[transform_csv_task, transform_db_task] >> merge

# La tarea de carga se ejecuta después del merge
#merge >> load_task

