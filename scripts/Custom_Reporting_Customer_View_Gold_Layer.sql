
--BUILD CUSTOM REPORT

/*

================================================

CUSTOMER REPORT

================================================

Purpose:
	- This report consolidates key customer metrics and behaviours


Highlights:
	-Gather essential fields such as names, ages and transaction details
	-Segments customers into categories (VIP, Regular, New) and age groups
	-Aggregates customer - level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan( in months)
	-Calculate valuable KPIs:
		-recency ( months since last order_
		-average order value
		-average monthly spent

*/

--USE DataWarehouseAnalytics;-- USE IN CASE IF NOT ACCESSIBLE TO DATABASE DataWarehouseAnalytics

CREATE VIEW gold.report_customers AS




WITH base_query AS
(

SELECT
	s.order_number,
	s.product_key,
	s.order_date,
	s.sales,
	s.quantity,
	c.customer_key,
	c.customer_number,
	CONCAT(c.first_name,' ',c.last_name) AS customer_name,
	DATEDIFF(YEAR,TRY_CAST(c.birthdate AS DATE),GETDATE()) AS customer_age
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON c.customer_key  = s.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
WHERE s.order_date IS NOT NULL
)
,
customer_aggregation AS
(
SELECT
	customer_key,
	customer_name,
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	customer_age,
	COUNT(DISTINCT order_number) AS total_orders_per_customer,
	SUM(sales) AS total_sales_per_customer,
	SUM(quantity) AS total_quantity,
	COUNT(product_key) AS total_products,
	DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS lifespan
FROM base_query
GROUP BY customer_key, customer_name,customer_age
--ORDER BY customer_key
)

SELECT
	customer_key,
	customer_name,
	customer_age,
	total_orders_per_customer,
	total_sales_per_customer,
	total_quantity,
	lifespan,
	CASE
		WHEN customer_age < 20 THEN 'Under 20'
		WHEN customer_age BETWEEN 20 AND 29 THEN '20-29'
		WHEN customer_age BETWEEN 30 AND 39 THEN '30-39'
		WHEN customer_age BETWEEN 40 AND 49 THEN '40-49'
		ELSE '> 50'
	END AS customer_age_category,

	CASE
		WHEN total_sales_per_customer > 5000 AND lifespan > 12 THEN 'VIP'
		WHEN total_sales_per_customer < 5000 AND lifespan > 12 THEN 'NORMAL'
		ELSE 'NEW'
	END AS customer_category,
	DATEDIFF(MONTH,last_order_date,GETDATE()) AS recency,
	CASE
	WHEN total_orders_per_customer = 0 THEN 0
	ELSE total_sales_per_customer/total_orders_per_customer
	END AS 'avg_sales_per_order',
	CASE 
		WHEN lifespan = 0 THEN total_sales_per_customer
		ELSE total_sales_per_customer/lifespan
	END AS avg_monthly_spent
FROM customer_aggregation;

SELECT
*
FROM gold.report_customers;

