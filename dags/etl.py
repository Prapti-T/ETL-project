from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'netflix_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False,
    description='ETL Netflix CSV to MySQL'
)

CSV_PATH = '/opt/airflow/data/netflix_titles.csv'  # Update path as per your setup

def extract():
    df = pd.read_csv(CSV_PATH)
    df.fillna('', inplace=True)
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce').dt.date
    return df.to_dict(orient='records')

def transform(**context):
    records = context['ti'].xcom_pull(task_ids='extract_data')
    cleaned = []
    for row in records:
        cleaned.append({
            'show_id': row['show_id'],
            'type': row['type'],
            'title': row['title'],
            'release_year': int(row['release_year']) if row['release_year'] else None,
            'rating': row['rating'],
            'duration': row['duration'],
            'date_added': row['date_added'],
            'description': row['description'],
            'director': row['director'].split(', ') if row['director'] else [],
            'cast': row['cast'].split(', ') if row['cast'] else [],
            'country': row['country'].split(', ') if row['country'] else [],
            'listed_in': row['listed_in'].split(', ') if row['listed_in'] else [],
        })
    return cleaned

def load(**context):
    data = context['ti'].xcom_pull(task_ids='transform_data')

    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='password',
        database='netflix',
        autocommit=True
    )
    cursor = conn.cursor()

    for row in data:
        cursor.execute("""
            INSERT IGNORE INTO shows (show_id, type, title, release_year, rating, duration, date_added, description)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (row['show_id'], row['type'], row['title'], row['release_year'], row['rating'], row['duration'], row['date_added'], row['description']))

        # Insert directors
        for d in row['director']:
            cursor.execute("INSERT IGNORE INTO directors (director_name) VALUES (%s)", (d,))
            cursor.execute("SELECT director_id FROM directors WHERE director_name = %s", (d,))
            dir_id = cursor.fetchone()[0]
            cursor.execute("INSERT IGNORE INTO show_directors (show_id, director_id) VALUES (%s, %s)", (row['show_id'], dir_id))

        # Insert actors
        for a in row['cast']:
            cursor.execute("INSERT IGNORE INTO actors (actor_name) VALUES (%s)", (a,))
            cursor.execute("SELECT actor_id FROM actors WHERE actor_name = %s", (a,))
            act_id = cursor.fetchone()[0]
            cursor.execute("INSERT IGNORE INTO show_actors (show_id, actor_id) VALUES (%s, %s)", (row['show_id'], act_id))

        # Insert countries
        for c in row['country']:
            cursor.execute("INSERT IGNORE INTO countries (country_name) VALUES (%s)", (c,))
            cursor.execute("SELECT country_id FROM countries WHERE country_name = %s", (c,))
            cou_id = cursor.fetchone()[0]
            cursor.execute("INSERT IGNORE INTO show_countries (show_id, country_id) VALUES (%s, %s)", (row['show_id'], cou_id))

        # Insert genres
        for g in row['listed_in']:
            cursor.execute("INSERT IGNORE INTO genres (genre_name) VALUES (%s)", (g,))
            cursor.execute("SELECT genre_id FROM genres WHERE genre_name = %s", (g,))
            gen_id = cursor.fetchone()[0]
            cursor.execute("INSERT IGNORE INTO show_genres (show_id, genre_id) VALUES (%s, %s)", (row['show_id'], gen_id))

    cursor.close()
    conn.close()

#Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task