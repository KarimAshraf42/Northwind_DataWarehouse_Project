# Northwind Data Warehouse Project

## Project Structure

```
Northwind_DataWarehouse_Project/
│
├── config.py                         ← Database connections (single source of truth)
├── utils.py                          ← Shared reusable functions
├── el.py                             ← Extract & Load (source → staging)
├── path_setup.py                     ← is used at the top of each ETL, Dimension, and Fact file to support standalone __main__ 
├── analysis.sql                      ← make some analysis on data warehouse
│
├── ETL/
│   ├── etl_categories.py
│   ├── etl_customers.py
│   ├── etl_employees.py
│   ├── etl_orders.py
│   ├── etl_order_details.py
│   ├── etl_products.py
│   ├── etl_shippers.py
│   └── etl_suppliers.py
│
├── Dimensions/
│   ├── dim_dates.py
│   ├── dim_customers.py
│   ├── dim_employees.py
│   ├── dim_suppliers.py
│   ├── dim_shippers.py
│   └── dim_products.py
│
└── Facts/
    └── fact_sales.py
```

## Pipeline Execution Order

```
1.  el.py                    ← Load staging from source
2.  ETL layer                ← Transform in memory
3.  dim_dates.py             ← No dependencies
4.  dim_shippers.py          ← No dependencies
5.  dim_customers.py         ← No dependencies
6.  dim_employees.py         ← No dependencies
7.  dim_suppliers.py         ← No dependencies
8.  dim_products.py          ← Depends on categories
9.  fact_sales.py            ← Depends on all dimensions
```

## Architecture

```
Source DB (northwind)
        ↓  el.py
Staging DB (northwind_staging)
        ↓  ETL layer (in memory)
Data Warehouse (northwind_data_warehouse)
   ├── dim_dates
   ├── dim_customers    (SCD Type 2)
   ├── dim_employees    (SCD Type 2)
   ├── dim_suppliers    (SCD Type 2)
   ├── dim_shippers
   ├── dim_products     (SCD Type 2)
   └── fact_sales
```

## Key Design Decisions

- **Star Schema** — one join per dimension from fact table
- **SCD Type 2** — customers, employees, suppliers, products
- **Degenerate Dimension** — order_id lives in fact_sales
- **Role-Playing Dimension** — dim_dates used for order_date, required_date, shipped_date
- **Category embedded in dim_products** — weak entity, low cardinality
- **Supplier as separate dimension** — strong independent entity
- **Designed and implemented range partitioning on fact_sales (1996–2000) along with strategic indexing on foreign keys and date columns across fact and dimension tables to optimize query performance, enable partition pruning, and support efficient analytical queries**.

## Shared Utilities (utils.py)

| Function | Purpose |
|---|---|
| `scd2_load(...)` | Generic SCD Type 2 loader |
| `simple_load(...)` | Simple load with duplicate protection |

