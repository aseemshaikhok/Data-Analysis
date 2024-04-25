-- How many Customers do we have in the data?
SELECT count(*) as Total_Customers from customers;

-- What was the city with the most profit for the company in 2015 and how much was it?
select DISTINCT shipping_city as City, sum(order_profits) as `Order Profit`
from orders 
join order_details
on orders.order_id = order_details.order_id
where order_date like '%2015%'
group by City
order by `Order Profit` desc;

-- How many different cities do we have in the data?
select count(DISTINCT shipping_city) as City from orders;

-- Show the total spent by customers from low to high. 
SELECT customer_name, sum(order_sales) as Total_Sales FROM customers
JOIN orders
On orders.customer_id = customers.customer_id
JOIN order_details
On orders.order_id = order_details.order_id
JOIN product
On order_details.product_id = product.product_id
group by customer_name
Order By Total_sales Desc;


-- What is the most profitable City in the State of Tennessee?
SELECT DISTINCT shipping_city, sum(order_profits)
FROM orders
JOIN order_details 
on orders.order_id = order_details.order_id
Where shipping_state like '%Tennessee%'
GROUP By shipping_city
ORDER BY sum(order_profits) DESC;


-- What’s the average annual profit for that city across all years in that city?
SELECT avg(order_profits) from order_details
Join orders
On orders.order_id = order_details.order_id
where shipping_city = 'Lebanon'; 

-- What is the distribution of customer types in the data?
SELECT DISTINCT customer_segment, count(customer_segment) FROM customers
GROUP BY customer_segment;

-- What’s the most profitable product category on average in Iowa across all years?
Select product_category, avg(order_profits) as AVG_Profit
FROM order_details
Join orders
ON order_details.order_id = orders.order_id
JOIN product
On product.product_id = order_details.product_id
WHERE shipping_state = 'Iowa'
GROUP BY product_category
ORDER BY AVG_Profit desc;

-- What is the most popular product in that category across all states in 2016? 
Select DISTINCT product_name, sum(quantity) as quantity, shipping_state from order_details
Join orders
ON order_details.order_id = orders.order_id
JOIN product
On product.product_id = order_details.product_id
WHERE order_date like '%2016%' 
AND product_category = 'Furniture'
GROUP BY product_name
ORDER BY quantity DESC;

-- Which customer got the most discount in the data? (in total amount)?
Select DISTINCT customer_name,order_sales,(order_sales*(1-order_discount)) as Amount_paid, (order_sales*order_discount) as Discount
From customers
JOIN orders
ON customers.customer_id = orders.customer_id
JOIN order_details
ON order_details.order_id = orders.order_id
JOIN product
On product.product_id = order_details.product_id
WHERE NOT Discount = 0
GROUP BY customer_name
ORDER BY Discount DESC;


-- How widely did monthly profits vary in 2018?
SELECT  
CAST(SUBSTR(order_date,INSTR(order_date,'/') -2,2) AS INT) AS month,
sum(order_sales) as order_sales
FROM order_details
JOIN orders
ON order_details.order_id = orders.order_id
WHERE order_date like '%2018%'
GROUP BY month
ORDER BY month;

