from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Get Variable from Airflow
param = Variable.get("postgres_neon_conn", deserialize_json=True)
db_conn = param["alchemy_conn"]  # Use SQLAlchemy connection string

# Default arguments for DAG
default_args = {
    'owner': '1',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Number of retries before failure
    'retry_delay': timedelta(seconds=2),
}

# Define DAG
with DAG(
    dag_id="dim_test",
    default_args=default_args,
    start_date=datetime(2024, 3, 16),
    schedule_interval="@once",  # Run only once
    catchup=False,
) as dag:

    # Task 1: Insert only new records
    insert_new_records = PostgresOperator(
        task_id="insert_new_records",
        postgres_conn_id="postgres_dna",  # Use Airflow connection ID
        sql="""
        INSERT INTO dm.dim_employee (employee_id, employee_first_name, employee_middle_initial, employee_last_name, employee_birth_date, employee_gender, employee_city_id, employee_hire_date)
     SELECT 
         em.employeeid,
         em.firstname,
         em.middleinitial,
         em.lastname,
         em.birthdate,
         em.gender,
         em.cityid,
         em.hiredate
        FROM stg.employee em;
        """
    )
    # Task 2: Insert customer country name and code
    update_insert_country = PostgresOperator(
        task_id="update_insert_city",
        postgres_conn_id="postgres_dna",
        sql="""
        WITH ec AS (
            SELECT
                em.cityid,
                ci.cityname,
                ci.countryid
            FROM stg.employee em
            LEFT JOIN stg.cities ci ON em.cityid = ci.cityid
        )
        UPDATE dm.dim_employee de
        SET 
            employee_city_name = ec.cityname,
            employee_country_id = ec.countryid
        FROM ec
        WHERE de.employee_city_id = ec.cityid;         
        """
    )

    # Task 3: Update customer_zipcode, customer_country_id
    update_insert_zip_id = PostgresOperator(
        task_id="update_insert_zip_id",
        postgres_conn_id="postgres_dna",
        sql="""
        update dm.dim_employee ec
        set 
            employee_country_name=co."CountryName",
            employee_country_code=co."CountryCode"
        FROM stg.countries co
        where ec.employee_country_id=co."CountryID"        
        """
    )

    # Task 4: Update existing records with missing insert_date
    update_insert_date = PostgresOperator(
        task_id="update_insert_date",
        postgres_conn_id="postgres_dna",
        sql="""
        UPDATE dm.dim_employee
        SET 
            start_date = CURRENT_DATE,
            end_date = NULL;
        """
    )

    # Task dependencies
    insert_new_records >> update_insert_country >> update_insert_zip_id >> update_insert_date