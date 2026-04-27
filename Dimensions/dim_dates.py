import path_setup
import pandas as pd
from config import dw_engine
from utils import simple_load
from ETL.etl_orders import run_etl_orders


def run_dim_dates():

    orders = run_etl_orders()

    all_dates = pd.concat([orders["order_date"],orders["required_date"],orders["shipped_date"]])

    min_date = all_dates.min()
    max_date = all_dates.max()

    print(f"Date range: {min_date.date()} → {max_date.date()}")

    dim_dates = pd.DataFrame({
        "full_date": pd.date_range(start=min_date, end=max_date)
    })

    dim_dates["date_key"]     = dim_dates["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_dates["day"]          = dim_dates["full_date"].dt.day
    dim_dates["day_of_week"]  = dim_dates["full_date"].dt.dayofweek + 1
    dim_dates["weekday_name"] = dim_dates["full_date"].dt.day_name()
    dim_dates["month"]        = dim_dates["full_date"].dt.month
    dim_dates["month_name"]   = dim_dates["full_date"].dt.month_name()
    dim_dates["quarter"]      = dim_dates["full_date"].dt.quarter
    dim_dates["year"]         = dim_dates["full_date"].dt.year
    dim_dates["is_weekend"]   = dim_dates["day_of_week"].isin([6, 7])

    dim_dates = dim_dates[[
        "date_key", "full_date", "day", "day_of_week",
        "weekday_name", "month", "month_name", "quarter",
        "year", "is_weekend"
    ]]

    simple_load(dim_dates, "dim_dates", "date_key", dw_engine())


if __name__ == "__main__":
    run_dim_dates()