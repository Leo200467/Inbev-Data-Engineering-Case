from airflow import DAG
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession, types
from airflow.operators.python import PythonOperator
import requests

# Configure DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start yesterday
}

with DAG(
    dag_id='fetch_brewery_data_dag',
    default_args=default_args,
    schedule='@daily',  # Change to your desired schedule
) as dag:

    def fetch_and_write_breweries(**kwargs):
        # Fetch brewery data
        url = 'https://api.openbrewerydb.org/v1/breweries'
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        breweries_data = response.json()

        # Get execution date
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')

        # Create SparkSession using provided connection ID (replace with your method)
        spark = SparkSession.builder.appName('write_brewery_data').getOrCreate()

        # Create a DataFrame from the fetched data
        schema = types.StructType([
            types.StructField("id", types.StringType(), True),  # Allow null values
            types.StructField("name", types.StringType(), True),
            types.StructField("brewery_type", types.StringType(), True),
            types.StructField("street", types.StringType(), True),
            types.StructField("address_1", types.StringType(), True),  # Address components may be null
            types.StructField("address_2", types.StringType(), True),
            types.StructField("address_3", types.StringType(), True),
            types.StructField("city", types.StringType(), True),
            types.StructField("state_province", types.StringType(), True),
            types.StructField("postal_code", types.StringType(), True),
            types.StructField("country", types.StringType(), True),
            types.StructField("longitude", types.StringType(), True),  # May be null
            types.StructField("latitude", types.StringType(), True),   # May be null
            types.StructField("phone", types.StringType(), True),
            types.StructField("website_url", types.StringType(), True),
            types.StructField("state", types.StringType(), True),  # Duplicate of state_province
        ])
        breweries_df = spark.createDataFrame(breweries_data, schema=schema)

        # Write DataFrame to CSV
        breweries_df.write.csv(
            path=f'/opt/airflow/data/bronze/brewery_data_{execution_date}.csv',
            header=True,
            mode='overwrite',
        )

        # Stop SparkSession
        spark.stop()

    fetch_and_write_task = PythonOperator(
        task_id='fetch_and_write_breweries',
        provide_context=True,
        python_callable=fetch_and_write_breweries,
        on_failure_callback=lambda context: context['task_instance'].xcom_push(
            key='error_msg', value=f"Error fetching or writing breweries: {context}"
        )
    )

