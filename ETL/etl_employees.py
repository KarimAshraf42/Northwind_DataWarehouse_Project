import path_setup
import pandas as pd
from config import staging_engine

def run_etl_employees():

    employees = pd.read_sql("SELECT * FROM employees", staging_engine())

    employees["last_name"] = (
        employees["last_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    employees["first_name"] = (
        employees["first_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    employees["title"] = (
        employees["title"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    employees["birth_date"] = pd.to_datetime(employees["birth_date"], errors="coerce")
    
    employees["hire_date"]  = pd.to_datetime(employees["hire_date"],  errors="coerce")

    employees["address"] = (
        employees["address"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    employees["city"] = (
        employees["city"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    employees["country"] = (
        employees["country"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .apply(lambda x: x.upper() if len(x) <= 3 else x.title())
    )

    employees["home_phone"] = (
        employees["home_phone"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    employees["extension"] = (
        employees["extension"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    employees = employees.rename(columns={"reports_to": "manager_id"})

    employees = employees.drop(
        columns=["title_of_courtesy", "region", "postal_code", "photo", "notes", "photo_path"]
    )

    return employees


if __name__ == "__main__":
    employees = run_etl_employees()
    print(employees)