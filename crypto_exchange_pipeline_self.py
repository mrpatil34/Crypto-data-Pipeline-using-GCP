import json
import requests
from datetime import datetime,timedelta
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator,BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook



#defining variables

GCP_PROJECT = "learn-airflow-436018"
GCS_BUCKET = "crypto-exchange-pipeline-rajeshwar"   #has tobe unique across google universe
GCS_RAW_DATA_PATH = "raw_data/crypto_raw_data"      # path where raw data will be stored
GCS_TRANSFORMED_PATH = "transformed_data/crypto_transformed_data"
BIGQUERY_DATASET = "crypto_db"
BIGQUERY_TABLE = "tbl_crypto"
BQ_SCHEMA = [
    {"name":"id","type":"STRING","mode":"REQUIRED"},
    {"name":"symbol","type":"STRING","mode":"REQUIRED"},
    {"name":"name","type":"STRING","mode":"REQUIRED"},
    {"name":"current_price","type":"FLOAT","mode":"NULLABLE"},
    {"name":"market_cap","type":"FLOAT","mode":"NULLABLE"},
    {"name":"total_volume","type":"FLOAT","mode":"NULLABLE"},
    {"name":"last_updated","type":"TIMESTAMP","mode":"NULLABLE"},
    {"name":"timestamp","type":"TIMESTAMP","mode":"REQUIRED"}

]



#extract data and xcom push
def _fetch_data_from_api(**kwargs):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency':"usd",
        'orders'     : 'market_cap_desc',
        'per_page'   : 10,
        'page'       : 1,
        'sparkline'  : False
    }

    response = requests.get(url,params=params)
    data = response.json()

    ts_nodash = kwargs['ts_nodash']
    filename = GCS_RAW_DATA_PATH + f"_{ts_nodash}"+".json"
    kwargs['ti'].xcom_push(key="crypto_filename",value=filename)
    kwargs['ti'].xcom_push(key="crypto_data",value=json.dumps(data))
  
#Upload data directly into GCS Bucket using Xcom pull
def _upload_raw_data_gcs(**kwargs):
    filename = kwargs['ti'].xcom_pull(task_ids="fetch_data_from_api",key="crypto_filename")
    data = kwargs['ti'].xcom_pull(task_ids="fetch_data_from_api",key="crypto_data")
    gcs_hook= GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=filename,
        data= data,
        mime_type='application/json'

    )

#Read data from GCS Bucket and Xcom push
def _read_raw_data_gcs(**kwargs):
    filename = kwargs['ti'].xcom_pull(task_ids="fetch_data_from_api",key="crypto_filename")
    gcs_hook= GCSHook(gcp_conn_id="google_cloud_default")
    data_content = gcs_hook.download(
        bucket_name=GCS_BUCKET,
        object_name=filename
    )

    data = data_content.decode('utf-8') 
    kwargs['ti'].xcom_push(key="crypto_data",value=json.loads(data))        #Xcom push for transform task


#process data using Xcom Pull and pass using Xcom push
def _transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids="read_raw_data",key="crypto_data")
    transformed_data = []

    for item in data:
        transformed_data.append({
            'id' : item['id'],
            'symbol' : item['symbol'],
            'name' : item['name'],
            'current_price' : item['current_price'],
            'market_cap': item['market_cap'],
            'total_volume' : item['total_volume'],
            'last_updated':item['last_updated'],
            'timestamp'  : datetime.utcnow().isoformat()

        })
    
    ts_nodash = kwargs['ts_nodash']
    df = pd.DataFrame(transformed_data)
    transform_filename = GCS_TRANSFORMED_PATH + f"_{ts_nodash}" + ".csv"
    transform_buffer = StringIO()
    df.to_csv(transform_buffer,index = False)
    transform_content = transform_buffer.getvalue()

    kwargs['ti'].xcom_push(key="crypto_transform_filename",value=transform_filename)
    kwargs['ti'].xcom_push(key="crypto_transform_data",value=transform_content)


#store transform data into GCS bucket using Xcom Pull
def _upload_processed_data_gcs(**kwargs):
    filename = kwargs['ti'].xcom_pull(task_ids="transformed_data",key="crypto_transform_filename")
    data = kwargs['ti'].xcom_pull(task_ids="transformed_data",key="crypto_transform_data")
    gcs_hook= GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=filename,
        data= data,
        mime_type='text/csv'

    )


default_args = {
    "owner" : "rajeshwar",
    "depends_on_past":False,

}

dag = DAG(
    dag_id = "crypto_exchange_pipeline_self",
    default_args=default_args,
    description="Fetch data from coingecko api",
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024,9,20),
    catchup=False
    
)

#fetch data from crypto api
fetch_data_task = PythonOperator(
    task_id="fetch_data_from_api",
    python_callable=_fetch_data_from_api,
    provide_context=True,
    dag=dag
)


#create gcs bucket
create_bucket_task = GCSCreateBucketOperator(
    task_id="create_bucket",
    bucket_name= GCS_BUCKET,
    storage_class="MULTI_REGIONAL",
    location="US",
    gcp_conn_id="google_cloud_default",
    dag=dag

)

#Upload raw data to gcs bucket
upload_raw_data_to_gcs_task = PythonOperator(
    task_id="upload_raw_data_to_gcs",
    python_callable=_upload_raw_data_gcs,
    provide_context = True,
    dag=dag
)


#Read raw data from gcs bucket
read_raw_data_task = PythonOperator(
    task_id="read_raw_data",
    python_callable=_read_raw_data_gcs,
    provide_context=True,
    dag=dag
)

#transformed data task
transformed_data_task= PythonOperator(
    task_id="transformed_data",
    python_callable = _transform_data,
    provide_context=True,
    dag=dag
)


#Upload transformed data to gcs bucket
upload_transformed_data_to_gcs_task = PythonOperator(
    task_id="upload_transformed_data_to_gcs",
    python_callable=_upload_processed_data_gcs,
    provide_context=True,
    dag=dag
)


#creating BIGQUERY DATASET (i.e database)
create_bigquery_dataset_task= BigQueryCreateEmptyDatasetOperator(
    task_id="create_bigquery_dataset",
    dataset_id = BIGQUERY_DATASET,
    gcp_conn_id="google_cloud_default",
    dag=dag
)


#creating BIGQUERY TABLE
create_bigquery_table_task = BigQueryCreateEmptyTableOperator(
    task_id="create_bigquery_table",
    dataset_id=BIGQUERY_DATASET,
    table_id=BIGQUERY_TABLE,
    schema_fields=BQ_SCHEMA,
    gcp_conn_id="google_cloud_default",
    dag=dag
)


#Load data to Bigquery table
load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects = [GCS_TRANSFORMED_PATH + "_{{ ts_nodash}}.csv"],
    destination_project_dataset_table=f'{GCP_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',     #have to add full path
    source_format="csv",
    schema_fields=BQ_SCHEMA,
    write_disposition="WRITE_APPEND",     #append data to table
    skip_leading_rows=1,
    gcp_conn_id="google_cloud_default",
    dag=dag

)



fetch_data_task >> create_bucket_task >> upload_raw_data_to_gcs_task >> read_raw_data_task

read_raw_data_task >> transformed_data_task >> upload_transformed_data_to_gcs_task

upload_transformed_data_to_gcs_task >> create_bigquery_dataset_task >> create_bigquery_table_task

create_bigquery_table_task >> load_to_bigquery


