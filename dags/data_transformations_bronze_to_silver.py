from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, translate, trim
import os
import unicodedata
import sys
import pytz
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

# Define timezone offset for UTC-3
timezone = pytz.timezone('America/Sao_Paulo')  

with DAG(
    dag_id='data_transformation_to_silver_layer_dag',
    start_date=datetime.now(),
    catchup=True,
    schedule_interval="@once",
    tags=['transformation'],
    is_paused_upon_creation=False,
) as dag:

    def make_trans():
        matching_string = ""
        replace_string = ""

        for i in range(ord(" "), sys.maxunicode):
            name = unicodedata.name(chr(i), "")
            if "WITH" in name:
                try:
                    base = unicodedata.lookup(name.split(" WITH")[0])
                    matching_string += chr(i)
                    replace_string += base
                except KeyError:
                    pass

        return matching_string, replace_string

    def clean_text(c):
        matching_string, replace_string = make_trans()
        return translate(
            regexp_replace(c, "\p{Pc}", ""), 
            matching_string, replace_string
        ).alias(c)

    bronze_directory = "/opt/airflow/data/bronze"
    silver_directory = "/opt/airflow/data/silver"

    def get_most_recent_bronze_directory():

        folders = [folder for folder in os.listdir(bronze_directory) if folder.startswith("brewery_data")]
        folders.sort(reverse=True)

        for folder in folders:
            folder_path = os.path.join(bronze_directory, folder)
            csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
            if csv_files:
                return folder_path

        raise Exception("No directory with CSV files found in the bronze directory.")

    def transform_brewery_data(*op_args, **kwargs):
       
        # Get execution date for folder name
        execution_date = kwargs['execution_date']
        execution_date = execution_date.astimezone(timezone).strftime('%Y-%m-%d-%H-%M')
        
        # Create folder for execution date and time
        silver_data_folder = f'/opt/airflow/data/silver/brewery_data_{execution_date}'
        os.makedirs(silver_data_folder, exist_ok=True)  # Create folder if it doesn't exist
        
        # Initialize SparkSession
        spark = SparkSession.builder.appName('BrewerySilverDataTransformation').getOrCreate()

        print(kwargs)
        print(*op_args)
        # Define read path
        bronze_data_path = f"{get_most_recent_bronze_directory()}/*.csv"

        # Read CSV data
        bronze_data_df = spark.read.csv(bronze_data_path, header=True)

        # Selecting columns for refined dataset

        column_selection = ['id', 'name', 'brewery_type', 'city', 'state', 'country', 'longitude', 'latitude']

        bronze_data_df_transient = bronze_data_df.select([col for col in column_selection])

        # Basic trimming and space character replacement

        bronze_data_df_transient = bronze_data_df_transient.withColumn(colName='city', col=regexp_replace(lower(col=trim(col('city').cast('string'))), ' ', '-')) \
                                                        .withColumn(colName='state', col=regexp_replace(lower(col=trim(col('state').cast('string'))), ' ', '-')) \
                                                        .withColumn(colName='country', col=regexp_replace(lower(col=trim(col('country').cast('string'))), ' ', '-'))

        # Edge cases of specific states, cities and names
        bronze_data_df_transient = bronze_data_df_transient.withColumn('state', regexp_replace('state', 'k�rnten','karnten'))
        bronze_data_df_transient = bronze_data_df_transient.withColumn('state', regexp_replace('state', 'nieder�sterreich','niederosterreich'))
        bronze_data_df_transient = bronze_data_df_transient.withColumn('city', regexp_replace('city', 'klagenfurt-am-w�rthersee','klagenfurt-am-worthersee'))
        bronze_data_df_transient = bronze_data_df_transient.withColumn('name', regexp_replace('name', 'Anheuser-Busch Inc ̢���� Williamsburg','Anheuser-Busch/Inbev Williamsburg Brewery'))
        bronze_data_df_transient = bronze_data_df_transient.withColumn('name', regexp_replace('name', 'Caf� Okei','Cafe Okei'))
        bronze_data_df_transient = bronze_data_df_transient.withColumn('name', regexp_replace('name', 'Wimitzbr�u','Wimitzbrau'))
        bronze_data_df_transient = bronze_data_df_transient.withColumn('name', regexp_replace('name', 'â','-'))

        # Cleansing of remaining texts of country, state and city, removing special characters.
        bronze_data_df_transient = bronze_data_df_transient.withColumn('city', clean_text('city')) \
                                                        .withColumn('state', clean_text('state')) \
                                                        .withColumn('country', clean_text('country'))

        # Partition and write data to Parquet in the silver layer
        bronze_data_df_transient.write.mode('overwrite').partitionBy('country', 'state').parquet(silver_data_folder)
    
        spark.stop()

        return silver_data_folder
    
    get_most_recent_bronze_directory_task = PythonOperator(
        task_id='get_most_recent_bronze_directory',
        python_callable=get_most_recent_bronze_directory,
        provide_context=True,
    )

    transform_brewery_data_task = PythonOperator(
        task_id='transform_brewery_data',
        provide_context=True,
        python_callable=transform_brewery_data,
        op_args={'bronze_directory': bronze_directory},
        on_failure_callback=lambda context: context['task_instance'].xcom_push(
            key='error_msg', value=f"Error transforming brewery data: {context}"
        )
    )

    trigger_gold_layer_agg_dag = TriggerDagRunOperator(
        task_id='trigger', 
        trigger_rule=TriggerRule.ALL_SUCCESS, 
        trigger_dag_id="creating_agg_view_gold_data_dag",
        reset_dag_run=True)

    get_most_recent_bronze_directory_task >> transform_brewery_data_task >> trigger_gold_layer_agg_dag
