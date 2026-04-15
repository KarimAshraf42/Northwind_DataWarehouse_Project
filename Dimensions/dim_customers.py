import path_setup
from config import dw_engine
from utils import scd2_load
from ETL.etl_customers import run_etl_customers


def run_dim_customers():

    customers = run_etl_customers()

    scd2_load(
        df           = customers,
        table_name   = "dim_customers",
        business_key = "customer_id",
        tracked_cols = [
            "company_name",
            "contact_name",
            "contact_title",
            "address",
            "city",
            "country",
            "phone"
        ],
        engine       = dw_engine()
    )


if __name__ == "__main__":
    run_dim_customers()