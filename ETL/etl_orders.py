import path_setup
import pandas as pd
from config import staging_engine

def run_etl_orders():

    orders = pd.read_sql("SELECT * FROM orders", staging_engine())

    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    
    orders["required_date"]  = pd.to_datetime(orders["required_date"],  errors="coerce")

    orders["shipped_date"] = pd.to_datetime(orders["shipped_date"], errors="coerce")
    
    orders = orders.rename(columns={"ship_via": "shipper_id"})

    orders = orders.drop(columns=[
        "freight", "ship_name", "ship_address",
        "ship_city", "ship_region", "ship_postal_code", "ship_country"
    ])

    return orders


if __name__ == "__main__":
    orders = run_etl_orders()
    print(orders)