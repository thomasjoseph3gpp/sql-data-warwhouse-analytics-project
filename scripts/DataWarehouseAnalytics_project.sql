USE DataWarehouseAnalytics;

--ADVANCED DATA ANALYTICS
/*

--This projectis mainly to perfomr various advances data analytics on the data  available in the gold layer created as part of DataWarehouse project
	--Analysis Includes
		1.Change Over Time Trends
		2.Cumulative Analysis
		3.Runnin count of orders and products
		4. Performance Analysis
		5.Part to whole analysis
		6.Data Segmentation
		7.Custom Reporting
		
*/



--Change Over Time Trends

--it is the way to check how measure changes or evolves over time
-- it helps tpo track and identify the seasonality in the data

-- sales change over date

SELECT
TOP 1 *
FROM gold.fact_sales;

SELECT
order_date,
sales
FROM gold.fact_sales
ORDER BY order_date;

SELECT
order_date,
SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY order_date
ORDER BY order_date;

SELECT
YEAR(order_date) AS order_year,
SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY YEAR(order_date) 
ORDER BY YEAR(order_date) 

SELECT
DATETRUNC(YEAR,order_date) AS order_year,
SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(YEAR,order_date)
ORDER BY DATETRUNC(YEAR,order_date)

SELECT
MONTH(order_date) AS order_month,
SUM(sales) AS monthly_sales
FROM GOLD.FACT_SALES
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date)

SELECT DATETRUNC(MONTH, order_date) AS order_month,
SUM(sales) AS monthly_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date)

--CUMULATIVE ANALYSIS

-- aggregates data progressively over time. it helps to understand whether our business is growing or declining.

--Calculate the total sales per month and the running total of sales over time

--to calculate the sales per month

USE DataWarehouseAnalytics;

SELECT
DATETRUNC(MONTH,order_date) AS Order_Month,
SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY DATETRUNC(MONTH,order_date);


-- to calculate the running total

SELECT
order_month,
total_sales,
SUM(total_sales) OVER( ORDER BY order_month) AS running_total
FROM
(

SELECT
DATETRUNC(MONTH,order_date) AS order_month,
SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(MONTH,order_date)

)t
ORDER BY order_month;

--to calculate the average and running average of sales

SELECT
DATETRUNC(MONTH,order_date) AS order_month,
AVG(sales) AS avg_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY DATETRUNC(MONTH,order_date)

-- to calculate the running average of sales over time

SELECT
order_month ,
avg_sales,
AVG(avg_sales) OVER(ORDER BY order_month ) AS running_avg_sales
FROM
(
SELECT
DATETRUNC(MONTH,order_date) AS order_month,
AVG(sales) AS avg_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(MONTH,order_date)
)T
ORDER BY order_month

--monthly count and running count of products sold

SELECT
DATETRUNC( MONTH,order_date) AS order_month,
COUNT(DISTINCT order_number) AS total_orders,
COUNT(product_key) AS total_products
FROM gold.fact_sales
GROUP BY DATETRUNC( MONTH,order_date)

--RUNNING COUNT OF ORDERS AND PRODUCTS

SELECT
order_month,
total_orders,
SUM(total_orders) OVER( ORDER BY order_month) AS running_count_orders,
total_products,
SUM(total_products) OVER( ORDER BY order_month) AS running_count_products
FROM
(
SELECT
DATETRUNC(MONTH,order_date) AS order_month,
COUNT(DISTINCT order_number) AS total_orders,
SUM(quantity) AS total_products
FROM gold.fact_sales
GROUP BY DATETRUNC(MONTH, order_date)
)T

ORDER BY order_month



SELECT
*,
SUM(total_sales) OVER( PARTITION BY order_year ORDER BY order_month) AS running_total_over_year
FROM
(
SELECT
DATETRUNC(YEAR, order_date) AS order_year,
DATETRUNC(MONTH, order_date) AS order_month,
SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY DATETRUNC(YEAR, order_date),DATETRUNC(MONTH, order_date)
)T

--PERFORMANCE ANALYSIS

--Analyze the yearly performance of products by comparing each products sale to both its average sales performance and previous year's sales.

SELECT
*,
total_sales - yearly_avg_sales AS yoy_sales_with_avg_comparison
FROM
(

SELECT
YEAR(order_date) AS order_year,
SUM(sales) AS total_sales,
AVG(SUM(sales)) OVER( ORDER BY YEAR(order_date)) AS yearly_avg_sales
FROM gold.fact_sales
GROUP BY YEAR(order_date)
)T
ORDER BY order_year


WITH yearly_sales AS
(

SELECT 
YEAR(s.order_date) AS order_year,
p.product_name,
SUM(s.sales) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY YEAR(s.order_date),p.product_name
)
SELECT *,
AVG(total_sales) OVER( PARTITION BY product_name) AS product_avg,
total_sales - AVG(total_sales) OVER( PARTITION BY product_name) AS sales_diff,
LAG(total_sales) OVER( PARTITION BY product_name ORDER BY order_year) AS last_year_sale,
total_sales - LAG(total_sales) OVER( PARTITION BY product_name ORDER BY order_year) as sales_lag,
CASE
	WHEN total_sales - LAG(total_sales) OVER( PARTITION BY product_name ORDER BY order_year)  > 0 THEN 'sales_improved'
	WHEN total_sales - LAG(total_sales) OVER( PARTITION BY product_name ORDER BY order_year)  < 0 THEN 'sales_declined'
	WHEN total_sales - LAG(total_sales) OVER( PARTITION BY product_name ORDER BY order_year)  = 0 THEN 'no_change_in_sales'
	ELSE NULL
END AS sales_lag_category,


CASE
	WHEN total_sales - AVG(total_sales) OVER( PARTITION BY product_name) > 0 THEN 'above_avg'
	WHEN total_sales - AVG(total_sales) OVER( PARTITION BY product_name) < 0 THEN 'below_avg'
	ELSE 'equal_to_avg'
END AS product_category


FROM yearly_sales
ORDER BY product_name,order_year

--PART TO WHOLE ANALYSIS

-- it is used to analyse the proportion of contribution of a aprticular thing to the whole.

--which category contribtes to the overall sales

WITH proportion_calculation AS
(

SELECT
category,
total_sales_by_category,
SUM(total_sales_by_category) OVER()  AS total_sales
FROM
(
SELECT
p.category,
SUM(s.sales) AS total_sales_by_category
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key = s.product_key
GROUP BY p.category
)T
)
SELECT
category,
total_sales_by_category,
ROUND((CAST(total_sales_by_category AS FLOAT)/CAST(total_sales AS FLOAT))*100,2) AS '% of contributuon'
FROM
proportion_calculation
WHERE total_sales_by_category IS NOT NULL;

--DATA SEGMENTATION

--segment products into cost ranges and count how many products fall into each segment

SELECT
TOP 1 *
FROM gold.fact_sales;

SELECT
*
FROM gold.dim_products
WHERE product_key  = 14;

SELECT
cost_category,
COUNT(cost_category) AS product_count
FROM
(

SELECT
product_name,
cost,
CASE
WHEN cost <100 THEN 'low'
WHEN cost BETWEEN 100 AND 1000 THEN 'medium'
WHEN cost > 1000 THEN 'high'
ELSE 'n/a'
END AS 'cost_category'
FROM gold.dim_products)T
GROUP BY cost_category

--Group customers into three segments based on their spending behaviour
	--vip: at least 12 months of history and spending more than 5000
	--regular: atleast 12 months of history but spending 5000 or less
	--new: lifespan less than 12 months

WITH customer_grouping AS
(
SELECT
	full_name,
	customer_key,
	total_spent,
	Age,
	CASE
	WHEN total_spent > 5000 AND Age > 12 THEN 'VIP'
	WHEN total_spent < 5000 AND Age > 12 THEN 'REGULAR'

	ELSE 'NEW'
END AS customer_category
FROM
(

SELECT
	CONCAT( c.first_name,' ',c.last_name) AS full_name,
	c.customer_key,
	SUM(s.sales) AS total_spent,
	DATEDIFF(MONTH,MIN(s.order_date),MAX(s.order_date)) AS Age
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s
ON c.customer_key = s.customer_key
GROUP BY CONCAT( c.first_name,' ',c.last_name),c.customer_key
)T
)
SELECT
customer_category,
COUNT(customer_key) AS total_customers
FROM 
customer_grouping
GROUP BY customer_category;

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

USE DataWarehouseAnalytics;


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
customer_count,
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
ORDER BY total_sales DESC

