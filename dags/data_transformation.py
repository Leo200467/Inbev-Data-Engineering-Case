# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, translate, trim
from os import listdir
import unicodedata
import sys

bronze_directory = "/opt/airflow/data/bronze"
silver_directory = "/opt/airflow/data/silver"

folders = [folder for folder in listdir(bronze_directory) if folder.startswith("brewery_data")]
most_recent_bronze_folder = max(folders)

# Initialize SparkSession
spark = SparkSession.builder.appName('BreweryDataTransformation').getOrCreate()

# Read all CSV files from the bronze folder
bronze_data_df = spark.read.csv(f'{bronze_directory + "/" + most_recent_bronze_folder}/*.csv', header=True)

## pip install unidecode

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
silver_data_path = silver_directory
bronze_data_df_transient.write.mode('overwrite').partitionBy('country', 'state').parquet(silver_data_path)

