import path_setup
import pandas as pd
from config import staging_engine

def run_etl_shippers():

    shippers = pd.read_sql("SELECT * FROM shippers", staging_engine())

    shippers["company_name"] = (
        shippers["company_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    shippers["phone"] = (
        shippers["phone"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    return shippers


if __name__ == "__main__":
    shippers = run_etl_shippers()
    print(shippers)