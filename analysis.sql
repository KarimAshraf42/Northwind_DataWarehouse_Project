-- =========================
-- Sales Overview
-- =========================

-- Total revenue generated from all sales
select sum(sales_amount) as total_revenue from fact_sales;

-- Total number of unique orders
select count(distinct order_id) as total_ordersfrom fact_sales;

-- Average order value (total revenue / number of orders)
select round(sum(sales_amount)/count(distinct order_id),2) as avg_order_value from fact_sales;

-- Total number of unique customers who made purchases
select count(distinct customer_key) as total_customers from fact_sales;

-- Monthly revenue
select d.month, sum(f.sales_amount) as revenue from fact_sales as f
inner join dim_dates as d
on f.order_date_key = d.date_key
group by d.month
order by d.month;

-- Revenue by product category
select p.category_name, sum(f.sales_amount) as revenue from fact_sales as f
inner join dim_products as p
on f.product_key = p.product_key and p.is_current = true
group by p.category_name
order by revenue desc;

-- Revenue by country (customer location)
select c.country, sum(f.sales_amount) as revenue from fact_sales as f
inner join dim_customers as c
on f.customer_key = c.customer_key and c.is_current = true
group by c.country
order by revenue desc;

-- Top 10 products by revenue
select p.product_name, sum(f.sales_amount) as revenue from fact_sales as f
inner join dim_products as p
on f.product_key = p.product_key and p.is_current = true
group by p.product_name
order by revenue desc
limit 10;


-- =========================
-- Customer Analysis
-- =========================

-- Total number of customers in the sales fact table
select count(distinct customer_key) as total_customers from fact_sales;

-- Number of orders per customer (only customers with more than 1 order)
select c.contact_name, count(distinct f.order_id) as total_orders from fact_sales as f
inner join dim_customers as c
on f.customer_key = c.customer_key and c.is_current = true
group by c.contact_name
having count(distinct order_id) > 1
order by total_orders;

-- Average number of orders per customer
select round(count(distinct order_id)/count(distinct customer_key),2) as avg_orders_per_customer from fact_sales;

-- Top companies by total revenue
select c.company_name, sum(f.sales_amount) as revenue from fact_sales as f
inner join dim_customers as c
on f.customer_key = c.customer_key and c.is_current = true
group by c.company_name
order by revenue desc
limit 10;

-- Number of customers per country
select c.country, count(distinct f.customer_key) as total_customers from fact_sales as f
inner join dim_customers as c
on f.customer_key = c.customer_key and c.is_current = true
group by c.country
order by total_customers desc;

-- Number of orders per customer
select customer_key, count(distinct order_id) as total_orders from fact_sales
group by customer_key
order by total_orders desc;


-- =========================
-- Shipping & Operations
-- =========================

-- On-time delivery percentage 
select round(100.0 * count(distinct case 
            when shipped_date_key <= required_date_key then order_id end) / count(distinct order_id),2) as on_time_rate_pct
from fact_sales
where shipped_date_key is not null;

-- Total late orders count 
select count(distinct order_id) from fact_sales where shipped_date_key > required_date_key;

-- Breakdown of orders by delivery status 
select
    case
        when shipped_date_key <= required_date_key then 'On-Time'
        else 'Late'
    end as delivery_status,
    count(distinct order_id) as order_count
from fact_sales
where shipped_date_key is not null
group by delivery_status;


-- =========================
-- Products & Suppliers
-- =========================

-- Count of active products
select count(discontinued) from dim_products where discontinued = false and is_current = true;

-- Count of discontinued products
select count(discontinued) from dim_products where discontinued = true and is_current = true;

-- Products below reorder level
select count(product_id) as below_reorder_level from dim_products where units_in_stock < reorder_level
and discontinued = false and is_current = true;

-- Inventory gap analysis (stock vs reorder level)
select product_name, units_in_stock, units_on_order, reorder_level,(units_in_stock - reorder_level) as stock_gap
from dim_products where is_current = true and discontinued = false
order by stock_gap asc
limit 15;

-- Number of products supplied by each supplier
select s.company_name as supplier_name, count(distinct f.product_key) as total_products
 from fact_sales f inner join dim_suppliers s
on f.supplier_key = s.supplier_key and s.is_current = true
group by s.company_name
order by total_products desc;

-- Product status distribution (Active vs Discontinued)
select 
    case 
        when discontinued = true then 'Discontinued'
        else 'Active'
    end as status,
    count(*) as product_count 
from dim_products
where is_current = true
group by status;

-- Supplier revenue contribution
select s.company_name as supplier_name, sum(f.sales_amount) as revenue 
from fact_sales as f inner join dim_suppliers as s
on f.supplier_key = s.supplier_key and s.is_current = true
group by supplier_name
order by revenue desc;


-- =========================
-- Employee Performance
-- =========================

-- Top employee by total revenue
select e.first_name || ' ' || e.last_name as employee_name, sum(f.sales_amount) as revenue 
from fact_sales as f inner join dim_employees as e
on f.employee_key = e.employee_key and e.is_current = true
group by employee_name
order by revenue desc
limit 1;

-- Top employee by number of orders handled
select e.first_name || ' ' || e.last_name as employee_name, count(distinct f.order_id) as total_orders 
from fact_sales as f inner join dim_employees as e
on f.employee_key = e.employee_key and e.is_current = true
group by employee_name
order by total_orders desc
limit 1;

-- Average orders per employee
select round(count(distinct order_id)/count(distinct employee_key),2) as avg_orders_per_employee from fact_sales;

-- Employee revenue ranking
select e.first_name || ' ' || e.last_name as employee_name, sum(f.sales_amount) as revenue 
from fact_sales as f inner join dim_employees as e
on f.employee_key = e.employee_key and e.is_current = true
group by employee_name
order by revenue desc;

-- Employee order volume ranking
select e.first_name || ' ' || e.last_name as employee_name, count(distinct f.order_id) as total_orders 
from fact_sales as f inner join dim_employees as e
on f.employee_key = e.employee_key and e.is_current = true
group by employee_name
order by total_orders desc;

-- Employee monthly performance (revenue trend)
select e.first_name || ' ' || e.last_name as employee_name, d.year, d.month, d.month_name, sum(f.sales_amount) as revenue
from fact_sales f inner join dim_employees e 
on f.employee_key = e.employee_key and e.is_current = true
inner join dim_dates d 
on f.order_date_key = d.date_key
group by employee_name, d.year, d.month, d.month_name
order by employee_name, d.year, d.month;

-- Employee performance by product category
select e.first_name || ' ' || e.last_name as employee_name, count(distinct f.order_id) as total_orders, p.category_name
from fact_sales as f inner join dim_employees as e
on f.employee_key = e.employee_key and e.is_current = true
inner join dim_products as p
on f.product_key = p.product_key
group by employee_name, p.category_name
order by total_orders desc;