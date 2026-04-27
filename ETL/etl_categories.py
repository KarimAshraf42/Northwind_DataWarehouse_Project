import path_setup
import pandas as pd
from config import staging_engine

def run_etl_categories():

    categories = pd.read_sql("SELECT * FROM categories", staging_engine())

    category_rename_map = {
        "Grains/Cereals": "Grains & Cereals",
        "Meat/Poultry":   "Meat & Poultry"
    }

    categories["category_name"] = (
        categories["category_name"]
        .replace(category_rename_map)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    categories = categories.drop(columns=["description", "picture"])

    return categories


if __name__ == "__main__":
    categories = run_etl_categories()
    print(categories)