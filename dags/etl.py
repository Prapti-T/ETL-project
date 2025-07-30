from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
import logging
from typing import List, Dict, Any

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'netflix_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False,
    description='ETL Netflix CSV to MySQL',
    tags=['netflix', 'etl', 'mysql'],
)

CSV_PATH = '/opt/airflow/data/netflix_clean.csv'

#For local env
# DB_CONFIG = {
#     'host': 'localhost',
#     'user': 'root',
#     'password': 'password',
#     'database': 'netflix',
#     'autocommit': True
# }

#For docker env
DB_CONFIG = {
    'host': 'mysql',  # the docker-compose service name
    'user': 'root',
    'password': 'password',
    'database': 'netflix',
    'autocommit': True,
}

def extract():
    try:
        logging.info(f'Starting to read CSV file from: {CSV_PATH}')
        df = pd.read_csv(CSV_PATH)
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce').dt.date
        records =  df.to_dict(orient='records')
        logging.info(f"Extraction completed successfully. Processed {len(records)} records")
        return records
    except FileNotFoundError:
        logging.error(f"CSV file not found at {CSV_PATH}")
        raise
    except Exception as e:
        logging.error(f"Error during extraction: {str(e)}")
        raise

def transform(**context):
    try:
        records = context['ti'].xcom_pull(task_ids='extract_data')
        logging.info(f"Starting transformation of {len(records)} records")
        cleaned = []
        skipped=0

        for index, row in enumerate(records):
            try:
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
            except Exception as e:
                logging.warning(f"Skipping record {index} due to error: {str(e)}")
                skipped += 1
                continue

        logging.info(f"Transformation completed. Processed: {len(cleaned)}, Skipped: {skipped}")
        return cleaned
    except Exception as e:
        logging.error(f"Error during transformation: {str(e)}")
        raise

def load(**context):
    conn=None
    try:
        data = context['ti'].xcom_pull(task_ids='transform_data')
        logging.info(f"Starting to load {len(data)} records to MySQL database")
        
        conn = mysql.connector.connect(DB_CONFIG)
        cursor = conn.cursor()
        
        shows_data = []
        for row in data:
            shows_data.append((
                row['show_id'], row['type'], row['title'], row['release_year'],
                row['rating'], row['duration'], row['date_added'], row['description']
            ))
        
        cursor.executemany("""
            INSERT IGNORE INTO shows (show_id, type, title, release_year, rating, duration, date_added, description)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, shows_data)
        logging.info(f"Inserted {len(shows_data)} shows")
        
        all_directors = set()
        all_actors = set()
        all_countries = set()
        all_genres = set()
        
        for row in data:
            all_directors.update(row['directors'])
            all_actors.update(row['cast'])
            all_countries.update(row['countries'])
            all_genres.update(row['listed_in'])
        
        all_directors = {d for d in all_directors if d and d.strip()}
        all_actors = {a for a in all_actors if a and a.strip()}
        all_countries = {c for c in all_countries if c and c.strip()}
        all_genres = {g for g in all_genres if g and g.strip()}
        
        if all_directors:
            cursor.executemany("INSERT IGNORE INTO directors (director_name) VALUES (%s)", 
                              [(d,) for d in all_directors])
            logging.info(f"Inserted {len(all_directors)} unique directors")
        
        if all_actors:
            cursor.executemany("INSERT IGNORE INTO actors (actor_name) VALUES (%s)", 
                              [(a,) for a in all_actors])
            logging.info(f"Inserted {len(all_actors)} unique actors")
        
        if all_countries:
            cursor.executemany("INSERT IGNORE INTO countries (country_name) VALUES (%s)", 
                              [(c,) for c in all_countries])
            logging.info(f"Inserted {len(all_countries)} unique countries")
        
        if all_genres:
            cursor.executemany("INSERT IGNORE INTO genres (genre_name) VALUES (%s)", 
                              [(g,) for g in all_genres])
            logging.info(f"Inserted {len(all_genres)} unique genres")
        
        director_map = {}
        if all_directors:
            cursor.execute("SELECT director_id, director_name FROM directors WHERE director_name IN ({})".format(
                ','.join(['%s'] * len(all_directors))), list(all_directors))
            director_map = {name: id for id, name in cursor.fetchall()}
        
        actor_map = {}
        if all_actors:
            cursor.execute("SELECT actor_id, actor_name FROM actors WHERE actor_name IN ({})".format(
                ','.join(['%s'] * len(all_actors))), list(all_actors))
            actor_map = {name: id for id, name in cursor.fetchall()}
        
        country_map = {}
        if all_countries:
            cursor.execute("SELECT country_id, country_name FROM countries WHERE country_name IN ({})".format(
                ','.join(['%s'] * len(all_countries))), list(all_countries))
            country_map = {name: id for id, name in cursor.fetchall()}
        
        genre_map = {}
        if all_genres:
            cursor.execute("SELECT genre_id, genre_name FROM genres WHERE genre_name IN ({})".format(
                ','.join(['%s'] * len(all_genres))), list(all_genres))
            genre_map = {name: id for id, name in cursor.fetchall()}
        
        show_directors_data = []
        show_actors_data = []
        show_countries_data = []
        show_genres_data = []
        
        for row in data:
            show_id = row['show_id']
            
            for director in row['directors']:
                if director in director_map:
                    show_directors_data.append((show_id, director_map[director]))
            
            for actor in row['cast']:
                if actor in actor_map:
                    show_actors_data.append((show_id, actor_map[actor]))
            
            for country in row['countries']:
                if country in country_map:
                    show_countries_data.append((show_id, country_map[country]))
            
            for genre in row['genres']:
                if genre in genre_map:
                    show_genres_data.append((show_id, genre_map[genre]))
        
        if show_directors_data:
            cursor.executemany("INSERT IGNORE INTO show_directors (show_id, director_id) VALUES (%s, %s)", 
                              show_directors_data)
            logging.info(f"Inserted {len(show_directors_data)} show-director relationships")
        
        if show_actors_data:
            cursor.executemany("INSERT IGNORE INTO show_actors (show_id, actor_id) VALUES (%s, %s)", 
                              show_actors_data)
            logging.info(f"Inserted {len(show_actors_data)} show-actor relationships")
        
        if show_countries_data:
            cursor.executemany("INSERT IGNORE INTO show_countries (show_id, country_id) VALUES (%s, %s)", 
                              show_countries_data)
            logging.info(f"Inserted {len(show_countries_data)} show-country relationships")
        
        if show_genres_data:
            cursor.executemany("INSERT IGNORE INTO show_genres (show_id, genre_id) VALUES (%s, %s)", 
                              show_genres_data)
            logging.info(f"Inserted {len(show_genres_data)} show-genre relationships")
        
        logging.info("Data loading completed successfully!")
        
    except Exception as e:
        logging.error(f"Error during loading: {str(e)}")
        raise
    finally:
        if conn:
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