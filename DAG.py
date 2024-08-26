# Import necessary libraries
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
import logging

# default args
default_args = {
    'owner': 'wisma',
    'start_date': dt.datetime(2024, 8, 16),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'flare_dag',
    default_args=default_args,
    description='Data for fetching from psql, cleaning, and posting to ElasticSearch',
    schedule_interval='30 6 * * *',
)


# task 1 : fetch data from PostgreSQL
def fetch_from_postgresql():
    # conn postgresql
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    # Fetch data from PostgreSQL
    data = pd.read_sql_query('SELECT * FROM table_m3', conn)
    data['created_at'] = data['created_at'].astype(str)
    file_path = '/opt/airflow/dags/data.csv'
    data.to_csv(file_path, index=False)
    return file_path


fetch_task = PythonOperator(
    task_id='fetch_from_postgresql',
    python_callable=fetch_from_postgresql,
    dag=dag,
)


# task 2 : data cleaning
def data_cleaning(file_path):
    data = pd.read_csv(file_path)
    data = drop_missing_value(data)
    data = drop_duplicate(data)
    data = remove_whitespace(data)
    data = rename_columns(data)
    data.to_csv('/opt/airflow/dags/data_clean.csv', index=False)


cleaning_task = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    op_kwargs={'file_path': '/opt/airflow/dags/data.csv'},
    dag=dag,
)


def drop_missing_value(data):
    # Drop missing value
    data.dropna(inplace=True)
    return data


def drop_duplicate(data):
    # Drop duplicate data
    data.drop_duplicates(inplace=True)
    return data


def remove_whitespace(data):
    # Remove whitespace
    data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return data


def rename_columns(data):
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('|', '')
    return data


# task 3 : post data to ElasticSearch

# Function to post data to ElasticSearch
def post_to_elasticsearch(file_path):
    try:
        es = Elasticsearch(['elasticsearch'],
                           port=9200,
                           )
        index_name = 'superstore'
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body={
                "mappings": {
                    "properties": {
                        "field1": {"type": "text"},
                        "field2": {"type": "keyword"}
                    }
                }
            })
            logging.info(f"Index '{index_name}' created.")

        df = pd.read_csv(file_path)

        for i, row in df.iterrows():
            doc = row.to_dict()
            es.index(index='superstore', doc_type="_doc", body=doc)
            logging.info(f"Document indexed: {doc}")

    except Exception as e:
        logging.error("An error occurred:", exc_info=True)


post_task = PythonOperator(
    task_id='post_to_elasticsearch',
    python_callable=post_to_elasticsearch,
    op_kwargs={'file_path': '/opt/airflow/dags/data_clean.csv'},
    dag=dag,
)

# Define task dependencies
fetch_task >> cleaning_task >> post_task
