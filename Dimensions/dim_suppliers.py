import path_setup
from config import dw_engine
from utils import scd2_load
from ETL.etl_suppliers import run_etl_suppliers


def run_dim_suppliers():

    suppliers = run_etl_suppliers()

    scd2_load(
        df           = suppliers,
        table_name   = "dim_suppliers",
        business_key = "supplier_id",
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
    run_dim_suppliers()