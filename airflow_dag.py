from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import requests
import boto3
import logging
from botocore.config import Config
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

endpoint_url = 'http://minio:9000'
aws_access_key_id = 'dataops'
aws_secret_access_key = 'Ankara06'
bucket_name = 'dataops'

def download_csvfile(hour, **kwargs):
    url = f'http://172.21.0.5:80/data/hour_{hour}_supermarket_sales.csv'
    file_name = f'/tmp/hour_{hour}_supermarket_sales.csv'
    try:
        response = requests.get(url)
        response.raise_for_status()  
        with open(file_name, 'wb') as file:
            file.write(response.content)
        return file_name
    except requests.exceptions.HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f'Error connecting: {conn_err}')
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f'Timeout error occurred: {timeout_err}')
    except requests.exceptions.RequestException as req_err:
        logging.error(f'An error occurred: {req_err}')
    return None

def get_s3_client():
    s3 = boto3.client('s3',
                      endpoint_url=endpoint_url,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      config=Config(signature_version='s3v4'))
    return s3

def save_df_to_s3(file_name, **kwargs):
    s3 = get_s3_client()
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"File {file_name} does not exist")
    
    try:
        df = pd.read_csv(file_name)
        today = datetime.now().strftime('%Y%m%d')
        hour = file_name.split('_')[1]
        key = f"supermarket_sales/{today}/hour_{hour}_supermarket_sales.csv"
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        logging.info(f"{key} saved to s3 bucket {bucket_name}")
    except Exception as e:
        logging.error(f"An error occurred while saving {file_name} to S3: {e}")
        raise e

with DAG('minio_s3_dag', 
         default_args=default_args, 
         description='A DAG to download CSVs and upload to Minio S3', 
         schedule_interval='@hourly', 
         start_date=datetime(2024, 6, 1), 
         catchup=False) as dag:

    for hour in range(1, 25):
        download_task = PythonOperator(
            task_id=f'download_csv_hour_{hour}',
            python_callable=download_csvfile,
            op_kwargs={'hour': hour},
            provide_context=True,
        )

        save_task = PythonOperator(
            task_id=f'save_csv_to_s3_hour_{hour}',
            python_callable=save_df_to_s3,
            op_kwargs={'file_name': f'/tmp/hour_{hour}_supermarket_sales.csv'},
            provide_context=True,
        )

        download_task >> save_task
