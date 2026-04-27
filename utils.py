import pandas as pd
from datetime import date
from sqlalchemy import text

# ── SCD Type 2 Load ───────────────────────────────────────────────────────────

def scd2_load(df, table_name, business_key, tracked_cols, engine):
    """
    Generic SCD Type 2 loader.

    Parameters
    ----------
    df           : transformed DataFrame ready to load
    table_name   : target table name in the data warehouse
    business_key : natural key column (e.g. 'customer_id')
    tracked_cols : list of columns to monitor for changes
    engine       : SQLAlchemy engine for the data warehouse
    """
    today = date.today()

    row_count = pd.read_sql(
        f"SELECT COUNT(*) AS cnt FROM {table_name}", engine
    )["cnt"].iloc[0]

    if row_count == 0:
        # ── FIRST LOAD ──
        print(f"First load detected — inserting all records into {table_name}.")

        df = df.copy()
        df["start_date"] = today
        df["end_date"]   = None
        df["is_current"] = True

        df.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"Inserted {len(df)} records.")

    else:
        # SUBSEQUENT LOAD 
        existing = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE is_current = TRUE", engine
        )

        merged = df.merge(
            existing[[business_key] + tracked_cols],
            on=business_key,
            how="left",
            suffixes=("_new", "_old")
        )

        # Detect CHANGED records
        changed_mask = False
        for col in tracked_cols:
            changed_mask = changed_mask | (
                merged[f"{col}_new"].fillna("").astype(str) !=
                merged[f"{col}_old"].fillna("").astype(str)
            )

        # Detect NEW records
        new_mask = merged[f"{tracked_cols[0]}_old"].isna()

        changed = merged[changed_mask & ~new_mask]
        new     = merged[new_mask]

        print(f"New records:     {len(new)}")
        print(f"Changed records: {len(changed)}")
        print(f"Unchanged:       {len(merged) - len(new) - len(changed)}")

        # Expire changed records
        if len(changed) > 0:
            changed_ids = changed[business_key].tolist()
            with engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE {table_name}
                    SET end_date   = :today,
                        is_current = FALSE
                    WHERE {business_key} = ANY(:ids)
                    AND   is_current     = TRUE
                """), {"today": today, "ids": changed_ids})
            print(f"Expired {len(changed_ids)} old records.")

        # Insert new versions for changed + brand new records
        to_insert = df[
            df[business_key].isin(new[business_key].tolist()) |
            df[business_key].isin(changed[business_key].tolist())
        ].copy()

        to_insert["start_date"] = today
        to_insert["end_date"]   = None
        to_insert["is_current"] = True

        if len(to_insert) > 0:
            to_insert.to_sql(
                table_name, engine, if_exists="append", index=False
            )
            print(f"Inserted {len(to_insert)} new/updated records.")

    print(f"{table_name} SCD Type II load completed.")


# ── Simple Load (no SCD) ──────────────────────────────────────────────────────

def simple_load(df, table_name, business_key, engine):
    """
    Smart simple load — inserts only NEW records.
    No SCD, no history — just new records detection.

    Parameters
    ----------
    df            : transformed DataFrame ready to load
    table_name    : target table name in the data warehouse
    business_key  : column or list of columns to detect new records
                    single column  -> "shipper_id" or "date_key"
                    composite key  -> ["order_id", "product_id"]
    engine        : SQLAlchemy engine for the data warehouse
    """

    # Handle both single and composite business keys
    if isinstance(business_key, str):
        keys = [business_key]
    else:
        keys = business_key

    existing = pd.read_sql(
        f"SELECT {', '.join(keys)} FROM {table_name}", engine
    )

    print(f"Existing records : {len(existing)}")
    print(f"Incoming records : {len(df)}")

    # Find records not in DW yet
    existing["_exists"] = True
    merged      = df.merge(existing, on=keys, how="left")
    new_records = merged[merged["_exists"].isna()].drop(columns=["_exists"])

    print(f"New records      : {len(new_records)}")

    if len(new_records) == 0:
        print(f"{table_name} — no new records to insert.")
    else:
        new_records.to_sql(
            table_name, engine, if_exists="append", index=False
        )
        print(f"Inserted {len(new_records)} new records into {table_name}.")

    print(f"{table_name} load completed.")