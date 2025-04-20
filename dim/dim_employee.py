from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import os
from datetime import datetime, timedelta
import pendulum


# Get Variable from Airflow
param = Variable.get("postgres_neon_conn", deserialize_json=True)
db_conn = param["alchemy_conn"]  # Use SQLAlchemy connection string

# Default arguments for DAG
default_args = {
    'owner': 'BAKTI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 8,  # Number of retries before failure
    'retry_delay': timedelta(seconds=2),
}

# Define DAG
with DAG(
    dag_id="dim_time",
    default_args=default_args,
    start_date=datetime(2024, 3, 16),
    schedule_interval="@once",  # Run only once
    catchup=False,
) as dag:

    # Task 1: membuat table
    insert_new_records = PostgresOperator(
        task_id="insert_new_records",
        postgres_conn_id="postgres_dna",  # Use Airflow connection ID
        sql="""
        CREATE TABLE dm.dim_time (
            sk_date SERIAL PRIMARY KEY,
            date DATE,
            days VARCHAR(40),
            month_id INTEGER,
            month_name VARCHAR(40),
            year INTEGER
        );

        INSERT INTO dm.dim_time(date, days, month_id, month_name, year)
        SELECT 
          days.d::DATE as date, 
          to_char(days.d, 'FMMonth DD, YYYY') as days, 
          to_char(days.d, 'MM')::integer as month_id, 
          to_char(days.d, 'FMMonth') as month_name, 
          to_char(days.d, 'YYYY')::integer as year
        from (
          SELECT generate_series(
            ('2000-01-01')::date, -- 'start' date
            ('2100-12-31')::date, -- 'end' date
            interval '1 day'  -- one for each day between the start and day
            )) as days(d);
        """
    )

    # Task dependencies
    insert_new_records
