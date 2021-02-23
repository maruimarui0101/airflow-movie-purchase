from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook # for uploading local files to GCS
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

import os, logging

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "start_date": datetime(2021, 2, 22), # we start at this date to be consistent with the dataset we have and airflow will catchup
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

# put vars here 
SOURCE_BUCKET = 'dj-max-event' 
DESTINATION_BUCKET = 'dj-max-event-landing' 
GOOGLE_CONN_ID = 'google-cloud-default' # flexibility of conection id if needed

unload_user_purchase ='./scripts/sql/filter_event_players.sql'
filtered_players_file = 'filtered_players.csv'
temp_filtered_user_purchase = f'/temp/{filtered_players_file}'

def local_to_gcs(
    destination_bucket=None
    , source_dir=None
    , source_object=None
    , prefix=None 
    , conn_id=GOOGLE_CONN_ID
    , **kwargs
    ):
    """
    Uploads local file to Google Cloud Storage and adds 
    """
    hook = GoogleCloudStorageHook()

    source_path = source_object
    if source_dir:
        source_path = f'{source_dir}/{source_object}'
  
    # labelling file with templated prefix
    if prefix:
        destination_object = '{}/{}'.format(prefix, source_object)

    hook.upload(destination_bucket, destination_object, source_path)

def remove_local_file(filelocation):
    if os.path.isfile(filelocation):
        os.remove(filelocation)
    else:
        logging.info(f'File {filelocation} not found')


with DAG("user_behaviour", default_args=default_args,
          schedule_interval="0 0 * * *", max_active_runs=1) as dag:

    # query data from pgres and unload it into file

    pg_unload = PostgresOperator(
        task_id='pg_unload',
        sql=unload_user_purchase,
        postgres_conn_id='postgres_default',
        params={'temp_filtered_user_purchase': temp_filtered_user_purchase},
        depends_on_past=True,
        wait_for_downstream=True
        )

    # take the file and upload to google cloud storage

    upload_file = PythonOperator(
        task_id='move_files'
        , python_callable=local_to_gcs
        , op_kwargs={
            'destination_bucket': 'dj-max-event' 
            , 'source_dir': '/temp'
            , 'source_object': filtered_players_file
            , 'prefix': "{{ts_nodash}}" 
        },
        provide_context=True
    )

    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data',
        bucket=SOURCE_BUCKET,
        source_objects=['{{ts_nodash}}/*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table='cm-airflow-tutorial-demo.djmax_event.players',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    # provision spark cluster
    # let us assume that some CI/CD is already taking care of the spark scripts elsewhere

    create_cluster = DataprocClusterCreateOperator(
    task_id='create_cluster'
    , project_id='cm-airflow-tutorial-demo'
    , cluster_name="spark-cluster-{{ ds_nodash }}"
    , num_workers=2
    , storage_bucket='cm-logistics-spark-bucket'
    , zone='us-east1-b'
    )

    # clean up 

    remove_local_player_file = PythonOperator(
    task_id='remove_local_user_purchase_file',
    python_callable=remove_local_file,
    op_kwargs={
        'filelocation': temp_filtered_user_purchase,
    }
    )

    # clean up cluster, save money! 

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster'
        , project_id='cm-airflow-tutorial-demo'
        , cluster_name='spark-cluster-{{ ds_noodash }}'
        , trigger_rule='all_done'
    )

    # dummy operator to signal end 

    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline')

pg_unload >> upload_file >> load_data >> remove_local_player_file >> end_of_data_pipeline