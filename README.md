## ABI Data Engineering – Breweries Case

This repository contains the code and configuration for a data pipeline that processes brewery data from the Open Brewery DB API and persists it into a data lake following the medallion architecture.

### Project Overview

This project demonstrates skills in:

* Consuming data from a API ["Open Brewery DB"](https://www.openbrewerydb.org/)
* Data transformation and cleansing with PySpark
* Orchestrating data pipelines with Apache Airflow
* Implementing the medallion architecture for data lakes (bronze, silver, gold layers)
* Containerization with Docker and Docker Compose

### Technologies Used

* Programming Language: Python
* Data Processing: PySpark
* Orchestration: Apache Airflow
* Containerization: Docker, Docker Compose

### Project Architecture

The following diagram represents the project architecture and its components:

!["Project Diagram"](/images/project-components.svg)

### Design decisions and review

#### Data Acquisition
* Data Format: Choosing CSV for the bronze layer allows for a simple, human-readable format during initial data ingestion.
* Requests library: Simple, easy to use and native from Python, Requests is the go-to choice for a open API

#### Data Profiling
* Profiling is essential: Before any transformations and treatment, a simple notebook for EDA was used to understand the data that is available. Some basic development was done with bronze CSVs data, resulting in insights on what data are we dealing with and improving the development plan of this project:
    1. `ID`, `name`, `brewery_type`, `city`, `country` and `state` are the only columns that are 100% filled, all other columns can have null values;
    2. `country` has some distinct values, however there is ` United States` and `United States` in the dataset, trimming values will be necessary;
    3. The dataset has `name`, `city` and `state` values with special characters, translation and replacement will also be necessary.

#### Data Transformation
* Data Cleansing and Schema Enforcement: Implementing data cleaning and schema enforcement in the silver layer ensures data quality and consistency for analytical purposes. Defining a more robust schema with data validation rules could further enhance data integrity.
* Data Cleansing: Implement data cleaning in the silver layer ensures data quality and consistency for analytical purposes.
* Partitioning: Partitioning data by country and state in the silver layer optimizes query performance in the gold layer, allowing for faster retrieval based on location. Additional partitioning by other relevant dimensions (e.g., brewery type) could be considered depending on anticipated analytical needs.

#### Data Aggregation (Gold Layer)
* Aggregation Approach: Creating an aggregated view and materializing it with brewery count by type and location (partitioned by country) offers a pre-computed and efficient way to access this data for analysis. Alternative aggregation options (e.g., state) could be explored based on specific analytical requirements.
* Denormalization: The gold layer view is using denormalized data, including brewery type, location and brewery count. This approach improves query performance by reducing the need for joins. Further denormalization or a fully normalized structure using Star-schema per Kimball books could be evaluated based on the complexity of future analytical queries.

### Storage Architecture

The storage adheres to the medallion architecture:

* **Bronze Layer:** Stores raw data fetched from the API in comma-separated values (CSV) format within the `bronze` directory.
* **Silver Layer:** Transforms data into Apache Parquet format for efficient storage and partitions it by country and state within the `silver` directory. Cleansing and schema enforcement are also applied in this layer.
* **Gold Layer:** Creates an aggregated view containing the quantity of breweries per type and location (partitioned by country) within the `gold` directory.

### Project Structure

```
Inbev-Data-Engineering-Case/
├── dags/
│   ├── fetch_data_with_meta_from_api.py
│   ├── data_transformations_bronze_to_silver.py
│   └── gold_data_aggregation.py
├── data/
│   ├── bronze
│   ├── silver
│   └── gold
├── logs/
├── plugins/
├── scripts/
│   ├── entrypoint.sh (Airflow entrypoint setup script)
├── airflow.env (Airflow env variables)
├── BEES Data Engineering - Breweries Case.md (Markdown format of case instructions)
├── docker-compose.yml (Docker Compose file for running all containers)
├── Dockerfile (Extends airflow image for extra setup)
├── README.md (this file)

```

### Project Overview

![image](/images/project-overview.svg)

1. Apache Airflow starts the ["fetch_data_with_meta_from_api"](/dags/fetch_data_with_meta_from_api.py). This DAG is responsible for extracting data from the Open Brewery DB API, starting from the Metadata endpoint `ttps://api.openbrewerydb.org/v1/breweries/meta`. The reason behind this is simple: Metadata offers information about total data available and provides a starting point for quality testing: we should always have the number of files equal to the total of objects divided by itens per_page requested. The DAG proceeds to call the `https://api.openbrewerydb.org/v1/breweries` endpoint, using the `per_page=200` parameter and a loop for getting each page.
2. In the same DAG, another component is responsible for saving each page data to the bronze layer. Each chunk is saved as a CSV inside the bronze layer in a named folder `brewery_data_{execution date}` (e.g. `brewery_data_2024-05-31-17-36`). The end of this task triggers the next DAG.
3. The following step is the ["data_transformations_bronze_to_silver"](/dags/data_transformations_bronze_to_silver.py) DAG. This is responsible for getting the most recent file folder in bronze layer into a Spark Dataframe and proceed with cleaning, transformations and formatting. The transformations that applied are:
    * Column selection: Only some columns were selected and keept in the dataset.
    * Trimming and characters replacement: Columns like `country` or `state` and even `name` had some problems with dangling space, out of UTF-8 characters and some edge cases like:
        * The state of Kärnten, where the `ä` character would appear as `k�rnten`, the behaviour is also observed in the [API Website itself](https://www.openbrewerydb.org/breweries/Austria/K%EF%BF%BDrnten);
        * Other edge cases were mapped and they are all treated in [data_transformations_bronze_to_silver.py](/dags/data_transformations_bronze_to_silver.py);
    * General character replacement and accents removal were done using `unidecode` package.
The same DAG writes the Dataframe into the silver layer, using Parquet as a format and partitioning by `country` and `state`.

4. Finnaly, the ["gold_data_aggregation"](/dags/gold_data_aggregation.py) DAG starts by getting the most recent silver data folder in the layer and using Spark aggregations to create a view by performing a group by operation using `brewery_type`, `country` and `state`, along with aggregation of the quantity of breweries, providing a analytical dataset in the gold layer available in Parquet format, partiotioned by `country`.

**Code comments throughout the DAGs provide explanations for data transformations and PySpark operations**

### Running the Pipeline

**Prerequisites:**

* Docker and Docker Compose installed

**Steps:**

1. Build and start Docker containers using `docker-compose up`.
2. Access the Airflow web UI (typically at `http://localhost:8080`) and trigger the ["fetch_data_with_meta_from_api"](/dags/fetch_data_with_meta_from_api.py) DAG.

### Monitoring and Alerting

While not explicitly implemented in this project, only Airflow webserver is used as monitoring for DAG success or failure, establishing a monitoring and alerting system for the pipeline is crucial for production environments. This could include:

* Airflow Monitoring: Leverage Airflow's built-in monitoring features to track DAG execution status, identify failures, and send alerts by email or messaging apps.
* Data Quality Checks: Implement data quality checks within the pipeline to ensure data integrity and consistency. Alerts can be triggered if data quality issues are detected.
* External Monitoring Tools: Consider integrating external monitoring tools like Prometheus or Datadog to provide comprehensive monitoring and alerting capabilities of the infrastructure system. 