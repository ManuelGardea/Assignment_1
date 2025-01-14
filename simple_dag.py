from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import time

# Define the functions for each task
def extract_data():
    print("Extracting data...")
    time.sleep(1)  # Simulate a delay
    return "Extracted Data"

def transform_data(extracted_data):
    print(f"Transforming data: {extracted_data}")
    time.sleep(1)  # Simulate a delay
    return f"Transformed {extracted_data}"

def load_data(transformed_data):
    print(f"Loading data: {transformed_data}")
    time.sleep(1)  # Simulate a delay
    return f"Loaded {transformed_data}"

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'simple_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline with Airflow',
    schedule_interval='@daily',  # Schedule the DAG to run daily
    start_date=datetime(2025, 1, 1),  # Start date for the DAG
    catchup=False,  # Do not backfill the DAG runs
) as dag:

    # Define the tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=['{{ task_instance.xcom_pull(task_ids="extract_data") }}'],  # Pass data between tasks
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=['{{ task_instance.xcom_pull(task_ids="transform_data") }}'],  # Pass data between tasks
    )

    # Set task dependencies (the order of execution)
    extract_task >> transform_task >> load_task
