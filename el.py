import pandas as pd
from sqlalchemy import inspect
from config import source_engine, staging_engine

def run_el():
    SOURCE_ENGINE = source_engine()
    STAGING_ENGINE = staging_engine()

    inspector = inspect(SOURCE_ENGINE)
    tables = inspector.get_table_names(schema="public")

    print(f"Found {len(tables)} tables in source: {tables}\n")

    failed = []

    for table_name in tables:
        try:
            df = pd.read_sql(
                f'SELECT * FROM public."{table_name}"',
                SOURCE_ENGINE
            )

            df.to_sql(
                table_name,
                STAGING_ENGINE,
                schema="public",
                if_exists="replace",
                index=False
            )

            print(f"Loaded table: {table_name}")

        except Exception as e:
            print(f"ERROR loading {table_name}: {e}")
            failed.append(table_name)

    print("\n--- Staging Load Summary ---")
    print(f"Loaded : {len(tables) - len(failed)} tables")
    print(f"Failed : {len(failed)} tables {failed if failed else ''}")