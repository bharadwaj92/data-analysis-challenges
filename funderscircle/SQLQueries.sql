/*DDL to create table Orders */

CREATE TABLE Orders ( Order_id  integer not null, Status varchar(50), Product_type varchar (50),  Region_name  varchar(20),  Order_amount integer ,  Order_date date, Delivery_date date ,PRIMARY KEY (Order_id));
       
/*DDL to create table Users */

CREATE TABLE Users (User_id integer not null, source varchar(50), date_registered date, PRIMARY KEY(User_id)); 

/*DDL to create table Users_Orders*/

CREATE TABLE Users_Orders (Order_id integer, User_id integer); 


/*•	What is the total volume and value of sales made each month? */

SELECT count(Order_id) AS total_volume, sum(Order_amount) AS total_value , 
str_to_date(concat(CONVERT(MONTH(Order_date), char(2)) , "/" , convert(YEAR(Order_date), char(4))), "%m/%Y") as yearmonth
FROM Orders 
WHERE Status = "order: delivered" OR Status = "order: shipping"
GROUP BY YEAR(Order_date), MONTH(Order_date)  ;

/*•	What proportion of registered users have made more than one order? */

SELECT IFNULL(
	(SELECT COUNT(uo.User_id)*100 / (SELECT COUNT(*) FROM Users) 
	FROM Users_Orders uo 
    GROUP BY uo.User_id 
    HAVING COUNT(uo.Order_id) > 1) 
,0) AS proportion; 



/*•	How has the conversion rate* from registered user to purchaser changed over time? Can you think of any reasons to explain the trends that you see?*/

SELECT count(temp.uid)*100/(SELECT count(*) FROM test.Users us WHERE YEAR(us.date_registered) = temp.y AND MONTH(us.date_registered) = temp.m ) AS CoversionRate,
str_to_date(concat(CONVERT(temp.m, char(2)) , "/" , convert(temp.y, char(4))), "%m/%Y") as yearmonth
FROM  
(
select u.User_id as uid, YEAR(u.date_registered) as y , MONTH(u.date_registered)  as m 
FROM test.Orders o 
INNER JOIN test.Users_Orders uo ON uo.Order_id = o.Order_id 
INNER JOIN test.Users u  
ON uo.User_id = u.User_id 
where o.Status <> "order: returned" )AS temp
GROUP BY temp.y, temp.m; 

/*Conversion rate of users who made an order within 30 days of registration*/

SELECT count(temp.uid)*100 / (SELECT count(*) FROM Users us WHERE YEAR(us.date_registered) = temp.y AND MONTH(us.date_registered) = temp.m ) AS CoversionRate,
str_to_date(concat(CONVERT(temp.m, char(2)) , "/" , convert(temp.y, char(4))), "%m/%Y") as yearmonth
FROM  
(
select u.User_id as uid, YEAR(u.date_registered) as y , MONTH(u.date_registered)  as m 
FROM Orders o 
INNER JOIN Users_Orders uo ON uo.Order_id = o.Order_id 
INNER JOIN Users u  
ON uo.User_id = u.User_id
WHERE o.Order_date BETWEEN u.date_registered AND ADDDATE(u.date_registered, 30) 
AND o.Status <> "order: returned" )AS temp
GROUP BY temp.y, temp.m; 