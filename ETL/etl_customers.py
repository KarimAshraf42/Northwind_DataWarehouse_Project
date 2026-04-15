import path_setup
import pandas as pd
from config import staging_engine

def run_etl_customers():
 
    customers = pd.read_sql("SELECT * FROM customers", staging_engine())

    customers["customer_id"] = (
        customers["customer_id"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.upper()
    )

    customers["company_name"] = (
        customers["company_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    customers["contact_name"] = (
        customers["contact_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    customers["contact_title"] = (
        customers["contact_title"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    customers["address"] = (
        customers["address"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    customers["city"] = (
        customers["city"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    customers["country"] = (
        customers["country"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .apply(lambda x: x.upper() if len(x) <= 3 else x.title())
    )

    customers["phone"] = (
        customers["phone"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    customers = customers.drop(columns=["region", "postal_code", "fax"])

    return customers


if __name__ == "__main__":
    customers = run_etl_customers()
    print(customers)