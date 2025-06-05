# Import necessary libraries
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define the SQL statement to create the table
# Using "CREATE TABLE IF NOT EXISTS" makes the operator idempotent,
# meaning it can be run multiple times without causing an error
# if the table already exists.
CREATE_PET_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS pets (
        pet_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        pet_type VARCHAR(50),
        birth_date DATE,
        owner VARCHAR(255)
    );
"""

# Define the DAG
with DAG(
    dag_id='create_table_dag_e',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',  # Run only once
    catchup=False,
    tags=['database', 'postgres', 'setup'],
    doc_md="""
    ### Create Table DAG Documentation
    This DAG runs once to create the `pets` table in the specified PostgreSQL database.
    - It uses the `PostgresOperator`.
    - **Important:** Ensure a PostgreSQL connection named `postgres_default` is configured in Airflow, or update the `postgres_conn_id` parameter in the task.
    - The `CREATE TABLE IF NOT EXISTS` statement ensures idempotency.
    """
) as dag:
    # Define the task using PostgresOperator
    create_pet_table = PostgresOperator(
        task_id='create_pet_table',
        postgres_conn_id='postgres_default',  # <<< IMPORTANT: Change this to your connection ID in Airflow
        sql=CREATE_PET_TABLE_SQL
    )

    # In this simple DAG, there's only one task, so no dependencies are needed.
    # If you had more tasks, you would define dependencies like:
    # task1 >> task2

# Note: No task dependencies needed here as there's only one task.
