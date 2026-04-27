import path_setup
from config import dw_engine
from utils import scd2_load
from ETL.etl_employees import run_etl_employees

def run_dim_employees():

    employees = run_etl_employees()

    scd2_load(
        df           = employees,
        table_name   = "dim_employees",
        business_key = "employee_id",
        tracked_cols = [
            "title",
            "address",
            "city",
            "home_phone",
            "extension",
            "manager_id"
        ],
        engine       = dw_engine()
    )


if __name__ == "__main__":
    run_dim_employees()