from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago


# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    table_creation,
    default_args=default_args,
    description='A simple DAG to create and load a BigQuery table',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Step 1: Create an empty table in BigQuery
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table',
        dataset_id='dataset_id',
        table_id='table_id',
        project_id='project_id',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
    )

    # Step 2: Load data from GCS to BigQuery
    load_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket='bucket_name',
        source_objects=['path/file.csv'],
        destination_project_dataset_table='project_id.dataset_id.table_id',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    create_bq_table >> load_gcs_to_bq
