from airflow.operators.python import PythonOperator
from airflow import DAG
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib.parse


def push(**kwargs):
    ds = kwargs['ds']

    # Load CSV
    df = pd.read_csv(f'/opt/airflow/shared/sport{ds}.csv')

    # Drop unwanted column
    df.drop(columns=['_id'], inplace=True)

    # Convert 'date' to datetime and keep day + month only
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # handle format issues safely
    df['Date'] = pd.to_datetime(df['Date'].dt.day.astype(str) + ' ' + df['Date'].dt.month.astype(str),
                                format='%d %m', errors='coerce')

    # Extract surnames from 'refree' column
    df['surnames'] = df['Referee'].str.split().str[-1]

    # Create DB connection
    password = urllib.parse.quote_plus("yam@110##")
    engine = create_engine(f'postgresql://postgres:{password}@host.docker.internal:5432/new_db')

    # Export to PostgreSQL
    df.to_sql('mongo_data', engine, if_exists='replace', index=False)

    print('CSV transformed and exported to PostgreSQL')


default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='import_dag',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["import"]
) as dag:
    read_csv = PythonOperator(
        task_id='import_csv',
        provide_context=True,
        python_callable=push,
        retries=3,
        retry_delay=timedelta(minutes=1) )
