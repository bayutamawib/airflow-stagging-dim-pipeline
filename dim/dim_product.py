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
    'retries': 8,  # Number of retries before failure
    'retry_delay': timedelta(seconds=2),
}

# Define DAG
with DAG(
    dag_id="dim_product_1",
    default_args=default_args,
    start_date=datetime(2024, 3, 16),
    schedule_interval="@once",  # Run only once
    catchup=False,
) as dag:

    
    # Task 1: Insert incremental values
    insert_incremental_values = PostgresOperator(
        task_id="insert_incremental_values",
        postgres_conn_id="postgres_dna", 
        sql="""
        CREATE SEQUENCE dm.dim_product_sk_product_seq2;
        ALTER TABLE dm.dim_product ALTER COLUMN sk_product SET DEFAULT nextval('dm.dim_product_sk_product_seq2');
        SELECT setval('dm.dim_product_sk_product_seq', COALESCE((SELECT MAX(sk_product) FROM dm.dim_product),1), FALSE);
        ALTER TABLE dm.dim_product ALTER COLUMN insert_date SET DEFAULT CURRENT_TIMESTAMP;
        """    
    )

    # Task 2: Insert only new records
    insert_new_records = PostgresOperator(
        task_id="insert_new_records",
        postgres_conn_id="postgres_dna",  # Use Airflow connection ID
        sql="""
        INSERT INTO dm.dim_product (product_id, product_name, product_price, category_id, category_name, modify_datetime, insert_date)
        SELECT 
            pr.ProductID,
            pr.ProductName,
            pr.Price,
            pr.CategoryID,
            ca.CategoryName,
            pr.ModifyDate,
            CURRENT_DATE
        FROM public.product pr
        LEFT JOIN public.demo11_dna_project_stg_categories ca ON pr.CategoryID = ca.CategoryID
        WHERE NOT EXISTS (
            SELECT 1 FROM dm.dim_product dp WHERE dp.product_id = pr.ProductID
        );
        """
    )
    
    # Task dependencies
    insert_incremental_values >> insert_new_records
