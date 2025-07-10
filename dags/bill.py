from datetime import datetime, timedelta
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable
from utils import append_to_whole_file, upload_to_minio, download_file_localy, process_date
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import aiohttp
import asyncio

bucket_name = 'billboard'
access_key = os.environ.get('MINIO_ACCESS_KEY', '')
secret_key = os.environ.get('MINIO_SECRET_KEY', '')

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

hot100_key = "hot-100/global/hot100.csv"
billboard200_key = "billboard-200/global/billboard200.csv"
digitalsongs_key = "digital-songs/global/digital_songs.csv"
streamingsong_key = "streaming-songs/global/streaming_songs.csv"
radio_key = "radio/global/radio.csv"

kaggle_folder = "/home/ubuntu/kaggle/billboard"

with DAG(
    'bill',
    default_args={
        'depends_on_past': True,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Project',
    schedule=CronTriggerTimetable("0 2 * * 3", timezone="CET"),
    start_date=datetime(2025, 6, 30),
    catchup=True,
    tags=['Billboard', 'Weekly'],
) as dag:
    dag.doc_md = """
        This is the dag of the big data project
        I can write documentation in Markdown here with **bold text** or __bold text__.
    """

    def get_date(**kwargs):
        current_date = kwargs['logical_date']
        date_str = current_date.strftime('%Y-%m-%d')
        return date_str

    def scrap(**kwargs):
        date_str = get_date(**kwargs)
        charts = kwargs.get('charts', None)
        asyncio.run(process_date(date_str, charts))

    def append_to_file(**kwargs):
        date_str = get_date(**kwargs)
        year, month, day = map(int, date_str.split('-'))
        charts = kwargs.get('charts', None)
        collection_key = kwargs.get('collection_key', None)

        collection_week_key = f"{charts}/{year:04d}/{month:02d}/{day:02d}/result.csv"
        append_to_whole_file(collection_key, collection_week_key)

    def download_file(**kwargs):
        file_path = kwargs.get('file_path', None)
        download_file_localy(s3_client, file_path)

    def upload_to_kaggle(**kwargs):
        api = KaggleApi()
        api.authenticate()
        api.dataset_create_version(kaggle_folder, version_notes = f"Updated on {get_date(**kwargs)}")

    scrap_hot100 = PythonOperator(
        task_id='scrap_hot100',
        python_callable=scrap,
        provide_context=True,
        op_kwargs={'charts': 'hot-100'}
    )

    complete_hot100 = PythonOperator(
        task_id='complete_hot100',
        python_callable=append_to_file,
        provide_context=True,
        op_kwargs={'charts': 'hot-100', 'collection_key': hot100_key}
    )

    download_hot100 = PythonOperator(
        task_id='download_hot100',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={'file_path': hot100_key}
    )


    scrap_billboard200 = PythonOperator(
        task_id='scrap_billboard200',
        python_callable=scrap,
        provide_context=True,
        op_kwargs={'charts': 'billboard-200'}
    )

    complete_billboard200 = PythonOperator(
        task_id='complete_billboard200',
        python_callable=append_to_file,
        provide_context=True,
        op_kwargs={'charts': 'billboard-200', 'collection_key': billboard200_key}
    )

    download_billboard200 = PythonOperator(
        task_id='download_billboard200',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={'file_path': billboard200_key}
    )


    scrap_radio = PythonOperator(
        task_id='scrap_radio',
        python_callable=scrap,
        provide_context=True,
        op_kwargs={'charts': 'radio-songs'}
    )

    complete_radio = PythonOperator(
        task_id='complete_radio',
        python_callable=append_to_file,
        provide_context=True,
        op_kwargs={'charts': 'radio-songs', 'collection_key': radio_key}
    )

    download_radio = PythonOperator(
        task_id='download_radio',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={'file_path': radio_key}
    )


    scrap_streaming = PythonOperator(
        task_id='scrap_streaming',
        python_callable=scrap,
        provide_context=True,
        op_kwargs={'charts': 'streaming-songs'}
    )

    complete_streaming = PythonOperator(
        task_id='complete_streaming',
        python_callable=append_to_file,
        provide_context=True,
        op_kwargs={'charts': 'streaming-songs', 'collection_key': streamingsong_key}
    )

    download_streaming = PythonOperator(
        task_id='download_streaming',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={'file_path': streamingsong_key}
    )


    scrap_digitalsong = PythonOperator(
        task_id='scrap_digitalsong',
        python_callable=scrap,
        provide_context=True,
        op_kwargs={'charts': 'digital-song-sales'}
    )

    complete_digitalsong = PythonOperator(
        task_id='complete_digitalsong',
        python_callable=append_to_file,
        provide_context=True,
        op_kwargs={'charts': 'digital-song-sales', 'collection_key': digitalsongs_key}
    )

    download_digitalsong = PythonOperator(
        task_id='download_digitalsong',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={'file_path': digitalsongs_key}
    )

    upload_to_kaggle = PythonOperator(
        task_id='upload_to_kaggle',
        python_callable=upload_to_kaggle,
        provide_context=True,
    )

    scrap_hot100 >> complete_hot100  >> download_hot100
    scrap_billboard200 >> complete_billboard200  >> download_billboard200
    scrap_radio >> complete_radio  >> download_radio
    scrap_digitalsong >> complete_digitalsong  >> download_digitalsong
    scrap_streaming >> complete_streaming  >> download_streaming

    [download_hot100, download_billboard200, download_radio, download_digitalsong, download_streaming] >> upload_to_kaggle