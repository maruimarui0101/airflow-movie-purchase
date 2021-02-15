from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook # for uploading local files to GCS

import os, logging

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "start_date": datetime(2010, 12, 1), # we start at this date to be consistent with the dataset we have and airflow will catchup
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# put vars here 
SOURCE_BUCKET = None 
DESTINATION_BUCKET = None
GOOGLE_CONN_ID = None # flexibility of conection id if needed

unload_user_purchase ='./scripts/sql/filter_unload_user_purchase.sql'
temp_filtered_user_purchase = '/temp/temp_filtered_user_purchase.csv'

def local_to_gcs(
    destination_bucket=None
    , source_object=None
    , prefix=None 
    , conn_id=GOOGLE_CONN_ID
    , **kwargs
    ):
    """
    Uploads local file to Google Cloud Storage and adds 
    """

    hook = GoogleCloudStorageHook()
    # labelling file with templated prefix
    if prefix:
        destination_object = '{}/{}'.format(prefix, destination_object)

    hook.upload(destination_bucket, destination_object, source_object)

def remove_local_file(filelocation):
    if os.path.isfile(filelocation):
        os.remove(filelocation)
    else:
        logging.info(f'File {filelocation} not found')


with DAG("user_behaviour", default_args=default_args,
          schedule_interval="0 0 * * *", max_active_runs=1) as dag:

    pg_unload = PostgresOperator(
        task_id='pg_unload',
        sql=unload_user_purchase,
        postgres_conn_id='postgres_default',
        params={'temp_filtered_user_purchase': temp_filtered_user_purchase},
        depends_on_past=True,
        wait_for_downstream=True
        )

    upload_file = PythonOperator(
        task_id='move_files'
        , python_callable=local_to_gcs
        , op_kwargs={
            'destination_bucket': None 
            , 'source_object': None
            , 'prefix': "{{ts_nodash}}" 
        },
        provide_context=True
    )

    remove_local_user_purchase_file = PythonOperator(
    task_id='remove_local_user_purchase_file',
    python_callable=remove_local_file,
    op_kwargs={
        'filelocation': temp_filtered_user_purchase,
    },
)


    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline')

pg_unload >> upload_file >> remove_local_user_purchase_file >> end_of_data_pipeline