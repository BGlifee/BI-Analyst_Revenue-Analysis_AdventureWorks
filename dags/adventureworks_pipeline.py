from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

def validate_kpi(**context):
    # 나중에 DB 읽어서 조건 검사
    print("Validate KPI")

def export_csv(**context):
    # 나중에 pyodbc/pandas로 CSV 저장
    print("Export CSV")

with DAG(
    dag_id="adventureworks_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    task_cleaning = SQLExecuteQueryOperator(
        task_id="cleaning",
        conn_id="mssql_default",
        sql="sql/cleaning/cleaning_checks.sql",
    )

    task_marts = SQLExecuteQueryOperator(
        task_id="marts",
        conn_id="mssql_default",
        sql=[
            "sql/marts/Dimension tables (date, customer, product).sql",
            "sql/marts/Fact table (sales).sql",
        ],
    )

    task_validation = PythonOperator(
        task_id="validation",
        python_callable=validate_kpi,
    )

    task_export = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv,
    )

    task_cleaning >> task_marts >> task_validation >> task_export