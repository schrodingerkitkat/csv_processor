from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from csv_processor import CSVProcessor
import logging

#set arguments for dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.today('UTC').subtract(days=1),
    'end_date': None,
    'email': ['logs@domain.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Instantiate the CSVProcessor with the specified directories
INPUT_FOLDER = "./input"
OUTPUT_FOLDER = "./output"
PROCESSED_FOLDER = "./processed"
METADATA_FOLDER = "./metadata"

processor = CSVProcessor(
    INPUT_FOLDER, OUTPUT_FOLDER, PROCESSED_FOLDER, METADATA_FOLDER
)

# define the logic for the PythonOperator
def process_csv_files():
    try:
        all_data = processor.process_all_files()
        logging.info('All CSV files processed successfully.')
    except Exception as e:
        logging.error(f"Unexpected error occurred during processing: {e}")

# define the DAG
dag = DAG(
    'csv_processor_dag',
    default_args=default_args,
    description='DAG to orchestrate CSV processing tasks',
    schedule='0 0 * * *',
)

# define the PythonOperator
process_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    dag=dag,
)

# set the operator sequence
process_files_task



