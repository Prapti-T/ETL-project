from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def extract():
    df = pd.read_csv('data/netflix_titles.csv')
    df.to_csv('/tmp/netflix_raw.csv', index=False)

def transform():
    df = pd.read_csv('/tmp/netflix_raw.csv')
    df.dropna(subset=['title', 'type', 'release_year'], inplace=True)
    df = df[df['type'] == 'Movie']
    df.to_csv('/tmp/netflix_clean.csv', index=False)

def load():
    df = pd.read_csv('/tmp/netflix_clean.csv')
    conn = sqlite3.connect('database/netflix.db')
    df.to_sql('movies', conn, if_exists='replace', index=False)
    conn.close()

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG('netflix_etl_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='extract', python_callable=extract)
    t2 = PythonOperator(task_id='transform', python_callable=transform)
    t3 = PythonOperator(task_id='load', python_callable=load)

    t1 >> t2 >> t3