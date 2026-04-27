# Libraries
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime, timedelta
import path_setup

# Import EL
from el import run_el

# Import Dimensions and Fact
from Dimensions.dim_dates import run_dim_dates
from Dimensions.dim_shippers import run_dim_shippers
from Dimensions.dim_customers import run_dim_customers
from Dimensions.dim_employees import run_dim_employees
from Dimensions.dim_suppliers import run_dim_suppliers
from Dimensions.dim_products import run_dim_products
from Facts.fact_sales import run_fact_sales

# # DAG Definition
@dag(dag_id='northwind_dag',
     description='pipeline of northwind_datawarehouse',
     start_date=datetime(2026,3,28),
     schedule=CronDataIntervalTimetable("0 00 * * 6"),
     catchup=True,
     dagrun_timeout=timedelta(seconds=90)

     )
def northwind_dag():

    # Task 1 — Extract & Load
    task_el = PythonOperator(
        task_id         = "el",
        python_callable = run_el
    )

    # Task 2 — Dimensions (run in PARALLEL after EL)
    task_dim_dates = PythonOperator(
        task_id         = "dim_dates",
        python_callable = run_dim_dates
    )

    task_dim_shippers = PythonOperator(
        task_id         = "dim_shippers",
        python_callable = run_dim_shippers
    )

    task_dim_customers = PythonOperator(
        task_id         = "dim_customers",
        python_callable = run_dim_customers
    )

    task_dim_employees = PythonOperator(
        task_id         = "dim_employees",
        python_callable = run_dim_employees
    )

    task_dim_suppliers = PythonOperator(
        task_id         = "dim_suppliers",
        python_callable = run_dim_suppliers
    )

    task_dim_products = PythonOperator(
        task_id         = "dim_products",
        python_callable = run_dim_products
    )

    # Task 3 — Fact Table (runs LAST)
    task_fact_sales = PythonOperator(
        task_id         = "fact_sales",
        python_callable = run_fact_sales
    )

    # Execution Order
    task_el >> [
        task_dim_dates,
        task_dim_shippers,
        task_dim_customers,
        task_dim_employees,
        task_dim_suppliers,
        task_dim_products
    ] >> task_fact_sales

northwind_dag()