import path_setup
import pandas as pd
from config import staging_engine

def run_etl_products():

    products = pd.read_sql("SELECT * FROM products", staging_engine())

    products["product_name"] = (
        products["product_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    products["discontinued"] = products["discontinued"].astype(bool)

    products = products.drop(columns=["quantity_per_unit"])

    return products


if __name__ == "__main__":
    products = run_etl_products()
    print(products)