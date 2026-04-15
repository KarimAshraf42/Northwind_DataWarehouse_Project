from airflow.providers.postgres.hooks.postgres import PostgresHook

def source_engine():
    hook = PostgresHook(postgres_conn_id="northwind")
    return hook.get_sqlalchemy_engine()

def staging_engine():
    hook = PostgresHook(postgres_conn_id="northwind_staging")
    return hook.get_sqlalchemy_engine()

def dw_engine():
    hook = PostgresHook(postgres_conn_id="northwind_data_warehouse")
    return hook.get_sqlalchemy_engine()

