from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
import os
from datetime import datetime, timedelta
import pendulum


# Get Variable from Airflow Variable
param = Variable.get("postgres_neon_conn",deserialize_json=True)
db_conn = param["jdbc_url"]

# Database connection details (update as per your setup)
DB_URL = db_conn
DB_TABLE = 'public.customers'  # Target table in the stg schema

# Source CSV file (update this path as per your setup)
SOURCE_FILE = '/opt/airflow/dags/input/20180508/20180508.customers.csv'  # Path to the uploaded CSV file

# Default arguments for the DAG
args = {
    'owner' : 'Anes',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 20, # unlimited retries
    'retry_delay': timedelta(seconds=2),
}

# Initialize a Spark session
def start_spark_session(SOURCE_FILE, DB_URL, DB_TABLE):

    spark = SparkSession.builder \
                .appName("spark_insert_customers") \
                .config("spark.jars", "/opt/airflow/plugins/postgresql_jdbc.jar") \
                .getOrCreate()
    
    try:
        df = spark.read.options(header=True, inferSchema=True, delimiter = ';').csv(SOURCE_FILE)

        # Write the DataFrame to the target database table using JDBC
        df.write \
            .format('jdbc') \
            .option('url', DB_URL) \
            .option('dbtable', DB_TABLE) \
            .option('driver', 'org.postgresql.Driver') \
            .mode('append') \
            .save()
    except Exception as e:
        print(str(e))
    finally:
        spark.stop()

# Define the Airflow DAG
with DAG(
    dag_id='Anes_stg_customers',
    default_args=args,
    schedule="50 19 * * *",
    start_date=pendulum.datetime(2024, 10, 5, tz="Asia/Jakarta"),
    catchup=False,
) as dag:

    task_start = EmptyOperator (
        task_id='task_start'
    )

    task_end = EmptyOperator(
        task_id='task_end'
    )

    # PythonOperator to run the Spark function
    task_csv2stg = PythonOperator(
        task_id='spark_csv2stg_customers',
        python_callable=start_spark_session,
        op_kwargs={
            'SOURCE_FILE': SOURCE_FILE,
            'DB_URL': DB_URL,
            'DB_TABLE': DB_TABLE
        }
    )

    # Define task order
    task_start >> task_csv2stg >> task_end