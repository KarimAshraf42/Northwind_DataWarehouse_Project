import path_setup
from config import dw_engine
from utils import simple_load
from ETL.etl_shippers import run_etl_shippers

def run_dim_shippers():

    shippers = run_etl_shippers()
    simple_load(shippers, "dim_shippers", "shipper_id", dw_engine())


if __name__ == "__main__":
    run_dim_shippers()