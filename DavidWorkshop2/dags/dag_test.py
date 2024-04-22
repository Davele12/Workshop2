from sqlalchemy import create_engine
from sqlalchemy import create_engine, String, Integer, Float, Boolean, DateTime
from sqlalchemy.dialects.postgresql import TIMESTAMP
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from store_ops import upload_to_drive

logging.basicConfig(level=logging.INFO)

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

def disposing_engine(engine):
    engine.dispose()

def query_data(sql_query):
    logging.info(f"Querying database with SQL: {sql_query}")
    engine = make_db_engine()
    with engine.connect() as connection:
        return pd.read_sql(sql_query, con=connection.connection)

def read_csv_spotify():
    return pd.read_csv('/opt/airflow/dags/files/spotify_dataset.csv')

def transform_csv(**kwargs):
    logging.info("Transforming CSV data")
    df = pd.read_csv('/opt/airflow/dags/files/spotify_dataset.csv')
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df.to_csv('/opt/airflow/dags/files/spotify_dataset2.csv')
 

def etl_process(sql_query):
    logging.info("Starting ETL process")
    df = query_data(sql_query)
    transformed_df = transform_data(df)
    transformed_df.to_csv('/opt/airflow/dags/files/transformed_df_grammy.csv')

def transform_data(df):
    logging.info("Applying data transformations")
    df['published_at'] = pd.to_datetime(df['published_at'])
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    df = df.drop(columns=['updated_at', 'img'])
    return df

def merge_task(**kwargs):
    ti = kwargs['ti']
    logging.info("Starting the merge task")

    spotify_data = pd.read_csv('/opt/airflow/dags/files/spotify_dataset2.csv')
    grammy_data = pd.read_csv('/opt/airflow/dags/files/transformed_df_grammy.csv')

    if spotify_data.empty or grammy_data.empty:
        logging.error("Data is empty")
        raise ValueError("Data is empty")

    spotify_data['artists_normalized'] = spotify_data['artists'].str.lower().str.strip()
    grammy_data['artist_normalized'] = grammy_data['artist'].str.lower().str.strip()
    spotify_data['track_name_normalized'] = spotify_data['track_name'].str.lower().str.strip()
    grammy_data['nominee_normalized'] = grammy_data['nominee'].str.lower().str.strip()

    merged_data = pd.merge(spotify_data, grammy_data, how='left', left_on='artists_normalized', right_on='artist_normalized')

    merged_data = pd.merge(merged_data, grammy_data, how='left', left_on='track_name_normalized', right_on='nominee_normalized', suffixes=('', '_nominee_match'))

    columns_to_drop = ['artists_normalized', 'artist_normalized', 'track_name_normalized', 'nominee_normalized']
    merged_data.drop(columns=columns_to_drop, inplace=True)

    logging.info(f"Merged data contains {len(merged_data)} records")

    merged_data.to_csv('/opt/airflow/dags/files/merged_data.csv', index=False)

def read_csv(path):
    return pd.read_csv(path)

def load_data(df, table_name):
    engine = make_db_engine()
    with engine.begin() as connection: 
        raw_conn = connection.connection
        if hasattr(raw_conn, 'cursor'):  
            cursor = raw_conn.cursor()
        try:
            df.to_sql(table_name, con=raw_conn, if_exists='replace', index=False)
            cursor.close()  
        except Exception as e:
            print(f"Error during SQL operation: {e}")
        finally:
            if not cursor.closed:
                cursor.close() 

def loading_data(path, table_name):
    df = read_csv(path)
    load_data(df, table_name)

def load():
    logging.info("Starting load procedure")
    df = pd.read_csv('/opt/airflow/dags/files/merged_data.csv')

    engine = make_db_engine()
    try:
        with engine.connect() as connection:
            df.to_sql('data_merged', con=connection, if_exists='replace', index=False)
            logging.info("Data loaded successfully into the database")
    finally:
        disposing_engine(engine)
        logging.info("Database engine disposed")

with DAG(
    'ETL_DAG',
    default_args=default_args,
    description='An ETL DAG for Spotify and Grammy data',
    schedule_interval=timedelta(days=1),
) as dag:

    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=query_data,
        op_kwargs={'sql_query': 'SELECT * FROM "grammyAwards_Airflow"'},
    )

    transform_db_task = PythonOperator(
        task_id='etl_process',
        python_callable=etl_process,
        op_kwargs={'sql_query': 'SELECT * FROM "grammyAwards_Airflow"'},
    )

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv_spotify,
    )

    transform_csv_task = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv,
    )

    merge_task_op = PythonOperator(
        task_id='merge_data',
        python_callable=merge_task,
    )

    load_merged_data = PythonOperator(
        task_id='load_merged_data',
        python_callable=loading_data,
        op_kwargs={'path':'/opt/airflow/dags/files/merged_data.csv', 'table_name': 'merged_data'},
    )

    drive_upload = PythonOperator(
        task_id='load_drive_task',
        python_callable=upload_to_drive,
         op_kwargs={'filename':'Merged_data', 'filepath': '/opt/airflow/dags/files/merged_data.csv'}
    )

    read_csv_task >> transform_csv_task
    read_db_task >> transform_db_task
    [transform_csv_task, transform_db_task] >> merge_task_op >> load_merged_data
    merge_task_op >> drive_upload