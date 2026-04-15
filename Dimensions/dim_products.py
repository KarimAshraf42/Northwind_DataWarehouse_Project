import path_setup
from config import dw_engine
from utils import scd2_load
from ETL.etl_products import run_etl_products
from ETL.etl_categories import run_etl_categories


def run_dim_products():

    products   = run_etl_products()
    categories = run_etl_categories()

    dim_products = (
        products
        .merge(categories, on="category_id", how="left")
    )[[
        "product_id",
        "product_name",
        "category_name",
        "unit_price",
        "units_in_stock",
        "units_on_order",
        "reorder_level",
        "discontinued"
    ]]

    scd2_load(
        df           = dim_products,
        table_name   = "dim_products",
        business_key = "product_id",
        tracked_cols = [
            "unit_price",
            "units_in_stock",
            "units_on_order",
            "reorder_level",
            "discontinued"
        ],
        engine       = dw_engine()
    )


if __name__ == "__main__":
    run_dim_products()