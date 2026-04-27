import path_setup
import pandas as pd
from config import staging_engine

def run_etl_order_details():

    order_details = pd.read_sql("SELECT * FROM order_details", staging_engine())

    # Validate discounts
    invalid_discounts = order_details[
        (order_details["discount"] < 0) |
        (order_details["discount"] > 1)
    ]
    print(f"Invalid discounts found: {len(invalid_discounts)}")

    # Feature engineering — sales amount
    order_details["sales_amount"] = (
        order_details["unit_price"] *
        order_details["quantity"] *
        (1 - order_details["discount"])
    ).round(2)

    return order_details


if __name__ == "__main__":
    order_details = run_etl_order_details()
    print(order_details)