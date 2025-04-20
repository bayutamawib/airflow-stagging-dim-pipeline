from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Get Variable from Airflow
param = Variable.get("postgres_neon_conn", deserialize_json=True)
db_conn = param["alchemy_conn"]  # Use SQLAlchemy connection string

# Default arguments for DAG
default_args = {
    'owner': 'Bayu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Number of retries before failure
    'retry_delay': timedelta(seconds=2),
}

# Define DAG
with DAG(
    dag_id="dim_customer",
    default_args=default_args,
    start_date=datetime(2024, 3, 16),
    schedule_interval="@once",  # Run only once
    catchup=False,
) as dag:

 # Task 1: Update existing records with missing insert_date
    update_insert_date = PostgresOperator(
        task_id="update_insert_date",
        postgres_conn_id="postgres_dna",
        sql="""
        ALTER TABLE dm.dim_customer
            ALTER COLUMN start_date SET DEFAULT CURRENT_TIMESTAMP,
            ALTER COLUMN end_date SET DEFAULT NULL;
        """
    )

    # Task 2: Insert only new records
    insert_new_records = PostgresOperator(
        task_id="insert_new_records",
        postgres_conn_id="postgres_dna",  # Use Airflow connection ID
        sql="""
        INSERT INTO dm.dim_customer (customer_id, customer_first_name, customer_middle_initial_name, customer_last_name, customer_address, customer_city_id)
        SELECT 
            cu."CustomerID",
            cu."FirstName",
            cu."MiddleInitial",
            cu."LastName",
            cu."Address",
            cu."CityID"
        FROM stg.customers cu;
        """
    )
    # Task 3: Insert customer country name and code
    update_insert_country = PostgresOperator(
        task_id="update_insert_country",
        postgres_conn_id="postgres_dna",
        sql="""
        WITH cn AS (
            SELECT
                ci.cityid,
                co."CountryName",
                co."CountryCode"
            FROM stg.countries co
            LEFT JOIN stg.cities ci ON ci.countryid = co."CountryID"
        )
        UPDATE dm.dim_customer dc
        SET 
            customer_country_name = cn."CountryName",
            customer_country_code = cn."CountryCode"
        FROM cn
        WHERE dc.customer_city_id = cn.cityid;        
        """
    )

    # Task 4: Update customer_zipcode, customer_country_id
    update_insert_zip_id = PostgresOperator(
        task_id="update_insert_zip_id",
        postgres_conn_id="postgres_dna",
        sql="""
        update dm.dim_customer dc
        set 
            customer_city_name=ci.cityname,
            customer_zipcode=ci.zipcode,
            customer_country_id=ci.countryid
        FROM stg.cities ci
        where dc.customer_city_id=ci.cityid        
        """
    )


    # Task dependencies
    update_insert_date >> insert_new_records >> update_insert_country >> update_insert_zip_id