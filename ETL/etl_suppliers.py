import path_setup
import pandas as pd
from config import staging_engine

def run_etl_suppliers():

    suppliers = pd.read_sql("SELECT * FROM suppliers", staging_engine())

    suppliers["company_name"] = (
        suppliers["company_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    suppliers["contact_name"] = (
        suppliers["contact_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    suppliers["contact_title"] = (
        suppliers["contact_title"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    suppliers["address"] = (
        suppliers["address"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    suppliers["city"] = (
        suppliers["city"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    suppliers["country"] = (
        suppliers["country"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .apply(lambda x: x.upper() if len(x) <= 3 else x.title())
    )

    suppliers["phone"] = (
        suppliers["phone"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    suppliers = suppliers.drop(columns=["region", "postal_code", "fax", "homepage"])

    return suppliers


if __name__ == "__main__":
    suppliers = run_etl_suppliers()
    print(suppliers)