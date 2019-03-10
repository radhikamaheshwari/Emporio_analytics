# Emporio_analytics

Problem Statement:

 
Use the dataset in the below URL and perform the following tasks.
 
 
Create a standalone spark job to fetch the data from the star schema, deformalize the records and write the results into a csv file
Create a standalone spark job to fetch the data from the star schema , aggregate the total Price by CustomerID and Month and write the results into a csv file. Make sure the calculation is done in SparkSQL.
Create a standalone spark job to get the absolute number of customers who ordered a certain product for each product in a transaction when the total transaction price is more than 50
 
 
https://relational.fit.cvut.cz/dataset/Northwind




Solution:

Prerequisites:-

Python 2.7, Apache Spark 2.7

SDK: Python 2.7, mysql-connector-java-5.1.45


Approach:

1. Connected to the mariadb tables using mysql connector driver and created a dataframe for each table. since the dataset is very small, set the parallelism to read as 1 (numPartitions=1).
2. Denormalized the data sets based on their relation shown in the problem statement URL and created the four dataframes after joining the related datasets as below:-
  a) DetailedOrders
  b) DetailedCustomers
  c) DetailedEmployees
  d) DetailedProducts
3. Added another column "total_paid_price" in DetailedOrdersDF with formula: total_paid_price = UnitPrice * Quantity - Discount.
4. Calculated Month from order date and doing a sum(total_paid_price), group by CustomerID,month
5. Count distinct CustomerIDs from the DetailedOrders where total_price_paid by customer for that product is > 50
