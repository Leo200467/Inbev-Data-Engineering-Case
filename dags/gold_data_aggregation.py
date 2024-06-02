from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytz
from datetime import datetime


# Define timezone offset for UTC-3
timezone = pytz.timezone('America/Sao_Paulo')

silver_directory = "/opt/airflow/data/silver"
gold_directory = "/opt/airflow/data/gold"

def get_most_recent_silver_directory():

    folders = [folder for folder in os.listdir(silver_directory) if folder.startswith("brewery_data")]
    folders.sort(reverse=True)  # Sort folders in descending order (newest first)

    for folder in folders:
        folder_path = os.path.join(silver_directory, folder)
        csv_files = [f for f in os.listdir(folder_path) if f.endswith("_SUCCESS")]
        if csv_files:
            return folder_path  # Return the first directory with parquet files

    raise Exception("No directory with parquet SUCCESS file found in the Silver directory.")


def gold_layer_agg_table_creation(**kwargs):

    # Get execution date for folder name
    execution_date = kwargs['execution_date']
    execution_date = execution_date.astimezone(timezone).strftime('%Y-%m-%d-%H-%M')

    # Create folder for execution date and time
    gold_data_folder = f'/opt/airflow/data/gold/brewery_data_{execution_date}'
    os.makedirs(gold_data_folder, exist_ok=True)

    # Initialize SparkSession
    spark = SparkSession.builder.appName("BreweryGoldDataTransformation").getOrCreate()

    # Read most recent silver data parquet
    silver_df = spark.read.parquet(get_most_recent_silver_directory())

    # Develop dataframe of counting breweries grouped by brewery type and location (country and state). 
    brewery_counts_df = silver_df.groupBy('brewery_type', col('country'), col('state')).agg(count('*').alias('brewery_count'))

    # Create or Replace view of dataframe
    brewery_counts_df.createOrReplaceGlobalTempView("brewery_counts")

    # List Spark catalog
    spark_data_catalog = spark.catalog.listTables("global_temp")

    print("Created aggregated view 'brewery_counts' with brewery counts by type and location (country, state and city).")
    print(f'Listing catalog on global_temp {spark_data_catalog}')

    # Save view as materialized parquet file partitioned by country
    brewery_counts_df.write.mode('overwrite').partitionBy('country').parquet(gold_data_folder)

    # Use brewery_counts_temp_table = spark.table("global_temp.brewery_counts") to invoke aggregated view

    return spark_data_catalog


with DAG(
    dag_id='creating_agg_view_gold_data_dag',
    start_date=datetime.now(),
    catchup=True,
    schedule_interval="@once",
    tags=['aggregation'],
    is_paused_upon_creation=False,
) as dag:
    
    get_most_recent_silver_directory_task = PythonOperator(
    task_id='get_most_recent_silver_directory',
    python_callable=get_most_recent_silver_directory,
    provide_context=True,
    on_failure_callback=lambda context: context['task_instance'].xcom_push(
        key='error_msg', value=f"Error transforming brewery data: {context}"
        )
    )

    create_aggregated_gold_layer_task = PythonOperator(
        task_id='create_aggregated_gold_layer',
        provide_context=True,
        python_callable=gold_layer_agg_table_creation,
        on_failure_callback=lambda context: context['task_instance'].xcom_push(
            key='error_msg', value=f"Error transforming brewery data: {context}"
        )
    )

    get_most_recent_silver_directory_task >> create_aggregated_gold_layer_task

