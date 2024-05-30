from airflow import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python import PythonOperator
import requests
from urllib.parse import urlencode
import csv
import pytz
import os

# Configure DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start yesterday
}

# Define timezone offset for UTC-3
timezone = pytz.timezone('America/Sao_Paulo')  # Adjust based on your specific location


def get_total_breweries():
    # Get initial metadata about breweries
    meta_url = 'https://api.openbrewerydb.org/v1/breweries/meta'
    meta_response = requests.get(meta_url)
    meta_response.raise_for_status()
    return int(meta_response.json()['total'])


def fetch_brewery_data_by_page(**kwargs):
    # Get execution date with hour and minute in UTC-3
    execution_date = kwargs['execution_date']
    execution_date = execution_date.astimezone(timezone).strftime('%Y-%m-%d-%H-%M')

    total_breweries = get_total_breweries()  # Fetch total breweries count

    # Create folder for execution date and time
    data_folder = f'/opt/airflow/data/bronze/brewery_data_{execution_date}'
    os.makedirs(data_folder, exist_ok=True)  # Create folder if it doesn't exist

    per_page = 200

    # Loop through each page to fetch data
    for page_number in range(1, total_breweries // per_page + (total_breweries % per_page > 0) + 1):
        # Build URL with pagination parameter
        url = f'https://api.openbrewerydb.org/v1/breweries?{urlencode({"per_page": per_page, "page": page_number})}'
        response = requests.get(url)
        response.raise_for_status()
        breweries_data = response.json()

        # Write data to a separate CSV file for each page
        data_file = f'{data_folder}/brewery_data_{page_number}.csv'
        with open(data_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)

            # Write header row
            header = [
                        'id',
                        'name',
                        'brewery_type',
                        'street',
                        'address_1',
                        'address_2',
                        'address_3',
                        'city',
                        'state_province',
                        'postal_code',
                        'country',
                        'longitude',
                        'latitude',
                        'phone',
                        'website_url',
                        'state'
                    ]
            csv_writer.writerow(header)

            # Write each brewery data as a row
            for brewery in breweries_data:
                row = [brewery.get(key) for key in header]  # Extract values based on header
                csv_writer.writerow(row)

        # Push XCom with progress update (page and total)
        xcom_key = f'brewery_data_progress_{execution_date}'
        xcom_value = {'page': page_number, 'total': total_breweries}
        kwargs['ti'].xcom_push(key=xcom_key, value=xcom_value)


with DAG(
    dag_id='fetch_all_brewery_data_dag',
    default_args=default_args,
    schedule='@daily',  # Change to your desired schedule
) as dag:

    fetch_brewery_data_task = PythonOperator(
        task_id='fetch_brewery_data_by_page',
        provide_context=True,
        python_callable=fetch_brewery_data_by_page,
        on_failure_callback=lambda context: context['task_instance'].xcom_push(
            key='error_msg', value=f"Error fetching brewery data: {context}"
        )
    )