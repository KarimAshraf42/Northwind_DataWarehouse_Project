import path_setup
import pandas as pd
from config import dw_engine
from utils import simple_load
from ETL.etl_orders import run_etl_orders
from ETL.etl_order_details import run_etl_order_details
from ETL.etl_products import run_etl_products

def run_fact_sales():

    # Run ETL
    orders        = run_etl_orders()
    order_details = run_etl_order_details()
    products  = run_etl_products()

    # Load surrogate keys from DW
    dim_products  = pd.read_sql("SELECT product_key,  product_id  FROM dim_products  WHERE is_current = TRUE", dw_engine())
    dim_customers = pd.read_sql("SELECT customer_key, customer_id FROM dim_customers WHERE is_current = TRUE", dw_engine())
    dim_employees = pd.read_sql("SELECT employee_key, employee_id FROM dim_employees WHERE is_current = TRUE", dw_engine())
    dim_shippers  = pd.read_sql("SELECT shipper_key,  shipper_id  FROM dim_shippers",                          dw_engine())
    dim_suppliers = pd.read_sql("SELECT supplier_key, supplier_id FROM dim_suppliers WHERE is_current = TRUE", dw_engine())

    # Build fact table
    fact_sales = order_details.merge(orders, on="order_id", how="left")

    # Join surrogate keys
    fact_sales = fact_sales.merge(dim_products,  on="product_id",  how="left")
    fact_sales = fact_sales.merge(dim_customers, on="customer_id", how="left")
    fact_sales = fact_sales.merge(dim_employees, on="employee_id", how="left")
    fact_sales = fact_sales.merge(dim_shippers,  on="shipper_id",  how="left")

    # Join supplier_key through product_id → supplier_id
    product_supplier = products[["product_id", "supplier_id"]].drop_duplicates()
    fact_sales = fact_sales.merge(product_supplier, on="product_id",  how="left")
    fact_sales = fact_sales.merge(dim_suppliers,    on="supplier_id", how="left")

    # Create date keys
    fact_sales["order_date_key"] = (
        fact_sales["order_date"]
        .dt.strftime("%Y%m%d").astype(int)
    )

    fact_sales["required_date_key"] = (
        fact_sales["required_date"]
        .dt.strftime("%Y%m%d").astype(int)
    )

    fact_sales["shipped_date_key"] = (
    fact_sales["shipped_date"]
    .apply(lambda d: int(d.strftime("%Y%m%d")) if pd.notna(d) else pd.NA)
    .astype("Int64")
)

    # Final column selection
    fact_sales = fact_sales[[
        "order_id",
        "product_key",
        "supplier_key",
        "customer_key",
        "order_date_key",
        "required_date_key",
        "shipped_date_key",
        "employee_key",
        "shipper_key",
        "unit_price",
        "quantity",
        "discount",
        "sales_amount"
    ]]

    simple_load(fact_sales, "fact_sales", ["order_id", "product_key","order_date_key"], dw_engine())

if __name__ == "__main__":
    run_fact_sales()