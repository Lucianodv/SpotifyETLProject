from spotipy.oauth2 import SpotifyOAuth
from datetime import timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
import pendulum

from spotify_etl import run_spotify_etl

default_args = { 
    'owner': 'airflow',
    'depends_on_past' : False,
    'start_date' : pendulum.today('UTC').add(days=-1),
    'email' : ['lcvenialgo@gmail.com'],
    'email_on_failure': False,
    'email_on_retry' : False,
    'retries' : 1,
    'retries_delay' : timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='DAG Process: Spotify download songs in database',
    schedule=timedelta(days=1),
)



run_etl = PythonOperator(
    task_id = 'whole_spotify_etl',
    python_callable= run_spotify_etl,
    dag=dag
)

run_etl