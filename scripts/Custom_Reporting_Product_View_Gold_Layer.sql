--BUILDING A PRODUCT REPORT

/*

=======================================================================================================

											Product Report

=======================================================================================================

Purpose:
	- This report consolidates key product metrics and behaviors

Highlights:
	1. Gather essential fields such as product name, category, subcategory, and cost.
	2.Segment products by revenue to identify the high-performers,mid-range and low- performers.
	3. Aggregates product level metrics:
		- total orders
		-total sales
		-total quantity sold
		-total customers(unique)
		-lifespan( in months)
	4. Calculates valuable KPIs:
		- recency ( months since last sale)
		-average order revenue (AVO)
		-average monthly revenue

=======================================================================================================

*/

-- CREATING THE BASE TABLE FOR FURTHER REPORT GENERATION

--USE DataWarehouseAnalytics;

CREATE VIEW gold.report_products AS

WITH base_query_products AS
(
SELECT
	s.order_number,
	s.product_key,
	s.customer_key,
	s.order_date,
	s.sales,
	s.quantity,
	s.price,
	p.product_id,
	p.product_name,
	p.category_id,
	p.category,
	p.subcategory,
	p.cost,
	AVG(s.sales) OVER( ) AS avg_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
)
,
product_aggregation AS
(
SELECT
	product_name,
	category,
	COUNT( DISTINCT customer_key) AS customer_count,
	COUNT(DISTINCT order_number) AS order_count,
	SUM(sales) AS total_sales,
	AVG(sales) AS avg_sales,
	SUM(quantity) AS total_quantity,
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order,
	DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS lifespan
FROM base_query_products
GROUP BY product_name,category

)
SELECT
	product_name,
	category,
	customer_count,
	order_count,
	total_sales,
	avg_sales,
	CASE
		WHEN customer_count < 100 THEN 'low-performer'
		WHEN customer_count BETWEEN 100 AND 500 THEN 'medium-performer'
		WHEN customer_count BETWEEN 500 AND 1500 THEN 'high-performer'
		WHEN customer_count > 1500 THEN 'extremely-high-performer'
		ELSE 'N/A'
	END AS product_performance_category,
	DATEDIFF(MONTH ,first_order,last_order) AS recency,
	total_sales/order_count AS avg_revenue_per_order
FROM product_aggregation


SELECT
*
FROM gold.report_products;
