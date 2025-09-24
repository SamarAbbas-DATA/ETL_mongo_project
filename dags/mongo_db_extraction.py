from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def export_to_csv(**kwargs):
    import pandas as pd
    from pymongo import MongoClient
    ds = kwargs['ds']
    client = MongoClient('mongodb://host.docker.internal:27017/')
    db= client['school']
    collection = db['sport']
    data = list(collection.find())
    df = pd.DataFrame(data)
    df.to_csv(f'/opt/airflow/shared/sport{ds}.csv', index=False)
    print(f"data extracted from MongoDB and saved to CSV for ds = {ds}")



with DAG(
    dag_id = 'mongodb_extraction',
    catchup = False,
    start_date = datetime(2025,7,1),
    schedule_interval = '@daily',
    default_args = {
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'email_on_failure': False,
        'email_on_retry': False,
    }
) as dag:
    extract_task  = PythonOperator(
        task_id = 'export_csv',
        python_callable = export_to_csv
    )