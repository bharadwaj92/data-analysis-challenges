/*
This is a Quickbooks Online (QBO) dataset which contains one row per customer per month.									
									
id: customer identifier									
subscription month: The month the customer became a paying subscriber of QBO									
month of: The month the row corresponds to									
new subscriber: 1 if the customer became a paying subscriber in that month, 0 otherwise									
open subscriber: 1 if the customer was a paying subscriber as of the end of that month, 0 otherwise									
net attrition: -1 if the customer cancelled their subscription in that month, 1 if the customer resubscribed in that month, 0 otherwise									
revenue: Revenue collected from the customer in that month									
product: Customer’s SKU in that month									
signup type: How the customer signed up for QBO.									
	Appstore - via Apple or Google’s app store								
	Buy Now - via a Buy Now discount								
	Retail - via a retail store offer								
	Trial - via a free trial								
	Wholesale - via an accountant and their wholesale discount)								
channel: Marketing acquisition channel									
*/
use stock_prices ;

drop table if exists intuit_case_study ;

select * into intuit_case_study from intuit_case_study_bckup;

-- revenue check 

select customer_id,
       sum(open_subscriber)
from 
intuit_case_study_bckup
group by customer_id 
having sum(revenue) = 0 and sum(open_subscriber) > 0
order by 2 desc

select * from intuit_case_study_bckup where customer_id = 

select sum(revenue) from intuit_case_study where customer_id = '407247334' order by month_of

--- imputing revenue when a customer is open 
update x 
set x.revenue = y.avg_rev
from 
intuit_case_study x 
inner join (
select x.*,
	   y.avg_rev
from 
(
select a.customer_id, 
	   a.month_of,
	   a.product 
from intuit_case_study a 
inner join 
(
select customer_id
from 
intuit_case_study
group by customer_id 
having sum(revenue) = 0 and sum(open_subscriber) > 0
)b on a.customer_id = b.customer_id 
where a.revenue = 0.0
)x
inner join (
select month_of, product, avg(revenue) as avg_rev  
from 
intuit_case_study
where revenue > 0 
group by month_of, product 
)y on x.month_of = y.month_of and x.product = y.product 
)y on x.customer_id = y.customer_id and x.month_of = y.month_of and x.product = y.product and open_subscriber = 1 ;

/*
-- making customer open when revenue > 0 
update x 
set x.open_subscriber = 1 
from intuit_case_study x 
where revenue > 0 and net_attrition = 0 
*/


-- some customers have intermittent 0 values when they subscribed 
/*
select * from intuit_case_study_bkcup where net_attrition = 0 and open_subscriber = 0 and new_subscriber =0 and revenue > 0 ; 

select new_subscriber, open_subscriber, net_attrition, sum(revenue), count(distinct customer_id) from intuit_case_study
group by new_subscriber, open_subscriber, net_attrition
order by 1 ,2 ,3 

-- updating new subscribers 
update x 
set new_subscriber = 1 
from 
intuit_case_study x where subscription_month = month_of;


select * from intuit_case_study where subscription_month > month_of and signup_type = 'Trial' order by 1 = -1 

select * from intuit_case_study where customer_id = '253421488' order by month_of

select * from intuit_case_study where customer_id = 157522


select a.customer_id,
		sum(case when month_of > '2017-01-01' then revenue end)
from 
intuit_Case_study a 
inner join (
select customer_id from intuit_case_study where net_attrition = -1 and month_of < '2017-01-01'
group by customer_id 
having count(*) = 1
)b on a.customer_id = b.customer_id
group by a.customer_id
having sum(case when month_of > '2017-01-01' then revenue end) > 0 
and sum(case when month_of > '2017-01-01' and net_attrition = -1 then 1 end) = 0
*/

/*
-- if attrition = -1 , then set revenue to 0 until next resubscription  
-- revenue after attrition month 
update x 
set x.revenue = 0 
from 
intuit_case_study  x 
inner join (
select * from (
select x.customer_id,
	   x.month_of as attrited_month,
	   dateadd(month, 1, x.month_of) as attrited_next_month,
	   row_number() over(partition by x.customer_id, x.month_of order by coalesce(y.month_of, '2021-12-31') ) as rno ,
	   coalesce(dateadd(month, -1, y.month_of), '2021-12-31') as resubscribed_month
from 
(
select customer_id,
       month_of
from 
intuit_case_study 
where net_attrition = -1
)x
left join 
(
select customer_id,
	   month_of
from 
intuit_case_study 
where net_attrition = 1
) y on x.customer_id = y.customer_id 
)a
where a.rno = 1 
)y on x.customer_id = y.customer_id and x.month_of between y.attrited_next_month and y.resubscribed_month;
*/

/*
open for next month - (new customer - new churned ) = (repeat for next month (prev month value)) - (churned customers - new churned customers)
example :

M1 New subs : 131 , New Churned : 3 , Total Churned : 3  Eligible Repeat for Next month : 128 
M2 New subs : 146 , New Churned : 6 , Total Churned : 12  Eligible Repeat for Next Month : 262 = 128 + 146 - 12  : 262   
*/
/*
-- assumptions not including trial customers into subscribed customers list 
select month_of,
	   available_customers,
	   trial_customers,
	   new_subscribers + coalesce(lag(repeat_for_next_month) over(order by month_of),0)  as active_customers_current_month,
	   available_customers - (new_subscribers + coalesce(lag(repeat_for_next_month) over(order by month_of),0) + resubscribed_customers) - trial_customers as total_customers_churned,
	   new_subscribers,
	   lag(repeat_for_next_month) over(order by month_of) as repeat_customers,
	   resubscribed_customers,
	   new_churned_customers as new_churned_customers_current_month,
	   churned_customers-new_churned_customers as repeat_churned_customers_current_month,
	   churned_customers as total_churned_customers_current_month,
	   total_revenue,
	   total_revenue/(new_subscribers + coalesce(lag(repeat_for_next_month) over(order by month_of),0) + resubscribed_customers) as arpu,
	   new_revenue,
	   repeat_revenue,
	   new_revenue/new_subscribers as apru_new,
	   repeat_revenue/lag(repeat_for_next_month) over(order by month_of) as arpu_repeat
from (
select month_of,
	   count(distinct customer_id) as available_customers, 
	   count(distinct case when new_subscriber = 0 and subscription_month > month_of then customer_id end) as trial_customers,
	   sum(new_subscriber) as new_subscribers,
	   sum(open_subscriber) as repeat_for_next_month,
	   count(distinct case when new_subscriber = 1 and net_attrition = -1 then customer_id end) as new_churned_customers,
	   count(distinct case when net_attrition = -1 then customer_id end) as churned_customers,
	   count(distinct case when net_attrition = 1 then customer_id end) as resubscribed_customers,
	   sum(revenue) as total_revenue,
	   sum(case when new_subscriber = 1 then revenue end) as new_revenue,
	   sum(case when new_subscriber = 0 and open_subscriber = 1 then revenue end) as repeat_revenue
from 
dbo.intuit_case_study 
group by month_of
)a
order by 1 ;
*/


select *
from 
(
select month_of,
	   count(distinct customer_id) as available_customers, 
	   count(distinct case when new_subscriber = 0 and subscription_month > month_of then customer_id end) as trial_customers,
	   sum(new_subscriber) as new_subscribers,
	   sum(open_subscriber) - sum(new_subscriber) + count(distinct case when net_attrition = -1 then customer_id end) as repeat_customers,
	   count(distinct case when net_attrition = 1 then customer_id end) as resubscribed_customers,  
	   count(distinct case when revenue > 0 or open_subscriber = 1 then customer_id end) as paying_customers,
	   sum(open_subscriber) as active_subscribers,
	   count(distinct case when new_subscriber = 1 and net_attrition = -1 then customer_id end) as new_churned_customers,
	   count(distinct case when net_attrition = -1 then customer_id end) as churned_customers,
	   sum(revenue) as total_revenue,
	   sum(case when new_subscriber = 1 then revenue end) as new_revenue,
	   sum(case when new_subscriber = 0 then revenue end) as repeat_revenue,
	   sum(revenue)/count(distinct case when revenue > 0 or open_subscriber = 1 then customer_id end) as arpc,
	   sum(case when new_subscriber = 1 then revenue end)/ sum(new_subscriber) as arpc_new,
	   sum(case when new_subscriber = 0 then revenue end)/nullif((sum(open_subscriber) - sum(new_subscriber) + count(distinct case when net_attrition = -1 then customer_id end)),0) as arpc_repeat
from 
dbo.intuit_case_study 
group by month_of
order by 1
)x

---- Acquisition analysis by channel 
select  month_of, 
		channel, 
		count(distinct customer_id) as new_subscriptions 
from dbo.intuit_case_study  
where new_subscriber = 1 
group by month_of, channel 
order by 1,2 ;

---- Acquisition analysis by signup type 
select  month_of, 
		count(distinct case when signup_type = 'AppStore' then customer_id end) as appstore_subscriptions,
		count(distinct case when signup_type = 'Buy Now' then customer_id end) as buynow_subscriptions,
		count(distinct case when signup_type = 'Retail' then customer_id end) as retail_subscriptions,
		count(distinct case when signup_type = 'Trial' then customer_id end) as trial_subscriptions,
		count(distinct case when signup_type = 'Wholesale' then customer_id end) as wholesale_subscriptions		 
from dbo.intuit_case_study  
where new_subscriber = 1 
group by month_of 
order by 1;

--- products that customers first subscribe to 
select month_of,
	   count(distinct case when product = 'Plus' then customer_id end) as plus_customers,
	   count(distinct case when product = 'Simple Start' then customer_id end) as simplestart_customers,
	   count(distinct case when product = 'Essentials' then customer_id end) as essentials_customers
from dbo.intuit_case_study 
where new_subscriber = 1 
group by month_of
order by 1 ;


select month_of,
	   count(distinct case when product = 'Plus' then customer_id end) as plus_customers,
	   count(distinct case when product = 'Simple Start' then customer_id end) as simplestart_customers,
	   count(distinct case when product = 'Essentials' then customer_id end) as essentials_customers,
	   count(distinct case when product = 'Plus' then customer_id end)*100.00/
	   (count(distinct case when product = 'Simple Start' then customer_id end) + count(distinct case when product = 'Plus' then customer_id end) + 
	   count(distinct case when product = 'Essentials' then customer_id end)) as pct_plus
from dbo.intuit_case_study 
where new_subscriber = 1 and signup_type = 'Wholesale'
group by month_of
order by 1; 

select --month_of,
	   count(distinct case when product = 'Plus' then customer_id end) as plus_customers,
	   count(distinct case when product = 'Simple Start' then customer_id end) as simplestart_customers,
	   count(distinct case when product = 'Essentials' then customer_id end) as essentials_customers
from dbo.intuit_case_study 
where new_subscriber = 1 
--group by month_of
--order by 1 

---- trial to subscription conversion -- no need to see because in data , all trials are subscribed 
select *
from dbo.intuit_case_study 
where subscription_month > month_of 


-- do customers upgrade or degrade 
with cte as (
select a.*, 
	   b.product as next_product, 
	   b.month_of as next_month,
	   row_number() over(partition by a.customer_id, b.product order by b.month_of) as rno 
from 
(
select customer_id, product, month_of
from dbo.intuit_case_study 
where new_subscriber = 1 
)a 
inner join dbo.intuit_case_study b on a.customer_id = b.customer_id and a.month_of < b.month_of and a.product <> b.product 
--order by 1 ,  5 
)
select product,
	   count(*) as total_customers,
	   count(distinct case when next_product = 'Simple Start' then customer_id end) as simplestart_changed_customers,
	   count(distinct case when next_product = 'Essentials' then customer_id end) as essentials_changed_customers,
	   count(distinct case when next_product = 'Plus' then customer_id end) as plus_changed_customers
from 
cte 
where rno = 1 
group by product  
order by 1 ;

--- Churn analysis 

select product,
	   count(distinct case when open_subscriber = 1 or revenue > 0 or new_subscriber = 1 then customer_id end) as total_customers,
	   count(distinct case when net_attrition = -1 then customer_id end) as attrited
from 
intuit_case_study 
group by product 

select signup_type,
	   product,
	   count(distinct case when open_subscriber = 1 or revenue > 0 or new_subscriber = 1 then customer_id end) as total_customers,
	   count(distinct case when net_attrition = -1 then customer_id end) - count(distinct case when net_attrition = 1 then customer_id end) as attrited
from 
intuit_case_study 
group by signup_type, product 

-- average lifetime by product 
select product,
	   avg(datediff(month,sub_date, last_month)) as avg_months_life,
	   min(datediff(month,sub_date, last_month)) as min_months_life,
	   max(datediff(month,sub_date, last_month)) as max_months_life
from 
(
select a.customer_id,
	   product,
	   min(subscription_month) as sub_date,
	   max(case when net_attrition = -1 then month_of end) as last_month
from 
intuit_case_study a
inner join 
(
select customer_id
from 
intuit_case_study
group by customer_id
having sum(net_attrition) = -1 and sum(open_subscriber) > 0   
)b on a.customer_id = b.customer_id
group by a.customer_id, product 
)c
group by product  

-- product usage trends -- either open subscriber or revenue > 0 
select month_of,
	   count(distinct case when product = 'Plus' then customer_id end) as plus_users,
	   count(distinct case when product = 'Essentials' then customer_id end) as essentials_users,
	   count(distinct case when product = 'Simple Start' then customer_id end) as simple_start_users,
	   sum(case when product = 'Plus' then revenue end) as plus_revenue,
	   sum(case when product = 'Essentials' then revenue end) as essentials_revenue,
	   sum(case when product = 'Simple Start' then revenue end) as simple_start_revenue
from dbo.intuit_case_study 
where (open_subscriber = 1 or revenue > 0 or net_attrition <> 0 )
group by month_of 
order by 1  

select month_of, count(distinct customer_id) from dbo.intuit_case_study 
where open_subscriber = 1
group by month_of order by 1 



-- cohorted retention Monthly 

select subscription_month,
	   count(distinct customer_id) as subscribed_customers,
	   count(distinct case when datediff(month,subscription_month, month_of) = 1 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M1_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 2 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M2_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 3 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M3_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 4 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M4_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 5 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M5_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 6 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M6_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 7 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M7_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 8 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M8_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 9 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M9_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 10 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M10_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 11 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M11_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 12 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M12_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 13 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M13_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 14 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M14_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 15 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M15_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 16 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M16_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 17 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M17_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 18 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M18_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 19 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M19_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 20 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M20_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 21 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M21_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 22 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M22_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 23 and open_subscriber = 1 then customer_id end)*1.00/count(distinct customer_id) as M23_retention
from
dbo.intuit_case_study 
group by subscription_month
order by 1 ;



select subscription_month,
	   count(distinct customer_id) as subscribed_customers,
	   count(distinct case when datediff(month,subscription_month, month_of) = 1 and (open_subscriber = 1 or revenue > 0)  then customer_id end)*1.00/count(distinct customer_id) as M1_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 2 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M2_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 3 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M3_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 4 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M4_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 5 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M5_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 6 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M6_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 7 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M7_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 8 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M8_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 9 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M9_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 10 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M10_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 11 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M11_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 12 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M12_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 13 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M13_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 14 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M14_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 15 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M15_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 16 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M16_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 17 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M17_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 18 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M18_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 19 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M19_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 20 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M20_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 21 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M21_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 22 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M22_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 23 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M23_retention
from
(
select a.*, b.product as subscription_product 
from dbo.intuit_case_study a 
inner join 
	(select customer_id, 
	 	    product 
	 from dbo.intuit_case_study 
	 where new_subscriber = 1
	 )b on a.customer_id = b.customer_id
)a  
--where subscription_product = 'Simple Start'
group by subscription_month
order by 1; 


--- winback 

select
	   --channel,
	   --product,
	   signup_type,
	   count(distinct a.customer_id)
from dbo.intuit_case_study a 
inner join (select customer_id from dbo.intuit_case_study  group by customer_id having sum(open_subscriber) >= 1 ) b on a.customer_id = b.customer_id
where net_attrition =-1 
group by
	   --channel,
	   signup_type
order by 2 desc
 


-- are trial customers without subscription counted as churned 
select customer_id
from 
 dbo.intuit_case_study a
 where a.signup_type = 'Trial' 
 group by customer_id 
 having sum(open_subscriber) = 0 and sum(new_subscriber) = 0 
 
 
 select * from intuit_case_study where customer_id = '215873021' order by month_of

select customer_id from intuit_case_study where signup_type = 'Trial' group by customer_id having sum(open_subscriber) > 1




select customer_id 
from dbo.intuit_case_study 
where net_attrition = - 1  

select * from intuit_case_study where customer_id = '446969964' order by month_of
 

---- subscription by product and channel 
select channel, product, count(distinct customer_id) from dbo.intuit_case_study where new_subscriber = 1   and product = 'Plus'
group by channel, product 
order by 3 desc


--- additional retention analysis :
select subscription_month,
	   count(distinct customer_id) as subscribed_customers,
	   count(distinct case when datediff(month,subscription_month, month_of) = 1 and (open_subscriber = 1 or revenue > 0)  then customer_id end)*1.00/count(distinct customer_id) as M1_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 2 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M2_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 3 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M3_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 4 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M4_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 5 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M5_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 6 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M6_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 7 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M7_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 8 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M8_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 9 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M9_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 10 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M10_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 11 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M11_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 12 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M12_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 13 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M13_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 14 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M14_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 15 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M15_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 16 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M16_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 17 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M17_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 18 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M18_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 19 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M19_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 20 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M20_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 21 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M21_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 22 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M22_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 23 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M23_retention
from
(
select a.*, b.product as subscription_product , b.signup_type as initial_signup_type, b.channel as acq_channel
from dbo.intuit_case_study a 
inner join 
	(select customer_id, 
	 	    product,
	 	    Signup_type,
	 	    channel 
	 from dbo.intuit_case_study 
	 where new_subscriber = 1
	 )b on a.customer_id = b.customer_id
)a  
where 
--subscription_product = 'Plus'
signup_type = 'Buy Now'
group by subscription_month
order by 1;


select product,
signup_type,
	   count(distinct customer_id)
from 
(select customer_id, 
	 	    product,
	 	    Signup_type,
	 	    channel 
	 from dbo.intuit_case_study 
	 where new_subscriber = 1
	 and subscription_month < '2016-12-01')a 
	 group by product, signup_type

select * from (
select *, row_number() over (partition by product order by subs desc) as rno 
from 
(
select product,
channel,
	   count(distinct customer_id) subs
from 
(select customer_id, 
	 	    product,
	 	    Signup_type,
	 	    channel 
	 from dbo.intuit_case_study 
	 where new_subscriber = 1
	 and subscription_month < '2016-12-01')a 
	 group by product, channel
)g
)h
where h.rno = 1


Simple Start	OA	166
Plus	OA	98
Essentials	OA	86

select channel, 
		count(distinct case when signup_type = 'AppStore' then customer_id end) as appstore_subscriptions,
		count(distinct case when signup_type = 'Buy Now' then customer_id end) as buynow_subscriptions,
		count(distinct case when signup_type = 'Retail' then customer_id end) as retail_subscriptions,
		count(distinct case when signup_type = 'Trial' then customer_id end) as trial_subscriptions,
		count(distinct case when signup_type = 'Wholesale' then customer_id end) as wholesale_subscriptions		 
from dbo.intuit_case_study  
where new_subscriber = 1 
group by month_of 
order by 1;



select month_of,
	   count(distinct case when product = 'Plus' then customer_id end) as plus_users,
	   count(distinct case when product = 'Essentials' then customer_id end) as essentials_users,
	   count(distinct case when product = 'Simple Start' then customer_id end) as simple_start_users
from dbo.intuit_case_study    
where new_subscriber = 1 and channel = 'Acct Assisted Sales' 
group by month_of 
order by 1;

select * from 
(
select *,
		row_number() over(partition by channel order by new_subs desc) as rno 
from 
(
select channel,
	   product,
	   count(distinct customer_id) as new_subs 
from dbo.intuit_case_study
where new_subscriber = 1 
group by channel , product
)a
)b 
where b.rno = 1


select year(month_of),
	   cha,
	   count(distinct customer_id) as new_subs 
from dbo.intuit_case_study
--where new_subscriber = 1 
group by year(month_of), channel


select year(month_of),
	   product,
	   avg(distinct revenue) as rev 
from dbo.intuit_case_study
--where new_subscriber = 1 
where revenue > 0
group by year(month_of), product;


select subscription_month,
	   avg(revenue) as subscribed_customers,
	   avg(case when datediff(month,subscription_month, month_of) = 1 and open_subscriber = 1 then  revenue end) as M1_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 2 and open_subscriber = 1 then  revenue end) as M2_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 3 and open_subscriber = 1 then  revenue end) as M3_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 4 and open_subscriber = 1 then  revenue end) as M4_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 5 and open_subscriber = 1 then  revenue end) as M5_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 6 and open_subscriber = 1 then  revenue end) as M6_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 7 and open_subscriber = 1 then  revenue end) as M7_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 8 and open_subscriber = 1 then  revenue end) as M8_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 9 and open_subscriber = 1 then  revenue end) as M9_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 10 and open_subscriber = 1 then revenue end) as M10_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 11 and open_subscriber = 1 then revenue end) as M11_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 12 and open_subscriber = 1 then revenue end) as M12_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 13 and open_subscriber = 1 then revenue end) as M13_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 14 and open_subscriber = 1 then revenue end) as M14_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 15 and open_subscriber = 1 then revenue end) as M15_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 16 and open_subscriber = 1 then revenue end) as M16_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 17 and open_subscriber = 1 then revenue end) as M17_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 18 and open_subscriber = 1 then revenue end) as M18_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 19 and open_subscriber = 1 then revenue end) as M19_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 20 and open_subscriber = 1 then revenue end) as M20_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 21 and open_subscriber = 1 then revenue end) as M21_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 22 and open_subscriber = 1 then revenue end) as M22_revenue,
	   avg(case when datediff(month,subscription_month, month_of) = 23 and open_subscriber = 1 then revenue end) as M23_revenue
from                                                                                                             
dbo.intuit_case_study 
group by subscription_month
order by 1 ;


select customer_id ,month_of, count(*)
from 
dbo.intuit_case_study 
group by customer_id ,month_of
having count(*) > 1 
order by 3 desc;


select year(month_of) ,channel ,signup_type, product ,avg(revenue)
from 
dbo.intuit_case_study 
where revenue > 0
group by  year(month_of), channel, signup_type, product
order by 1,2,3,4;



select month_of ,signup_type, product ,avg(revenue)
from 
dbo.intuit_case_study 
where revenue > 0 and signup_type in ('Buy Now','Wholesale','Trial') and product <> 'Simple Start'
group by month_of, signup_type, product
order by 1,2,3;



select channel,
       signup_type,
       
from 
dbo.intuit_case_study
where new_subscriber = 1  
group by channel , signup_type 



select signup_type,
	   product,
	   channel,
	   avg(case when M1_retention > 0.0 then M1_retention end) as m1_re_avg,
	   avg(case when M2_retention > 0.0 then M2_retention end) as m2_re_avg,
	   avg(case when M3_retention > 0.0 then M3_retention end) as m3_re_avg,
	   avg(case when M4_retention > 0.0 then M4_retention end) as m4_re_avg,
	   avg(case when M5_retention > 0.0 then M5_retention end) as m5_re_avg,
	   avg(case when M6_retention > 0.0 then M6_retention end) as m6_re_avg,
	   avg(case when M7_retention > 0.0 then M7_retention end) as m7_re_avg,
	   avg(case when M8_retention > 0.0 then M8_retention end) as m8_re_avg,
	   avg(case when M9_retention > 0.0 then M9_retention end) as m9_re_avg,
	   avg(case when M10_retention > 0.0 then M10_retention end) as m10_re_avg,
	   avg(case when M11_retention > 0.0 then M11_retention end) as m11_re_avg,
	   avg(case when M12_retention > 0.0 then M12_retention end) as m12_re_avg,
	   avg(case when M13_retention > 0.0 then M13_retention end) as m13_re_avg,
	   avg(case when M14_retention > 0.0 then M14_retention end) as m14_re_avg,
	   avg(case when M15_retention > 0.0 then M15_retention end) as m15_re_avg,
	   avg(case when M16_retention > 0.0 then M16_retention end) as m16_re_avg,
	   avg(case when M17_retention > 0.0 then M17_retention end) as m17_re_avg,
	   avg(case when M18_retention > 0.0 then M18_retention end) as m18_re_avg,
	   avg(case when M19_retention > 0.0 then M19_retention end) as m19_re_avg,
	   avg(case when M20_retention > 0.0 then M20_retention end) as m20_re_avg,
	   avg(case when M21_retention > 0.0 then M21_retention end) as m21_re_avg,
	   avg(case when M22_retention > 0.0 then M22_retention end) as m22_re_avg,
	   avg(case when M23_retention > 0.0 then M23_retention end) as m23_re_avg
from 
(
select subscription_month,
	   product, 
	   signup_type,
	   channel,
	   count(distinct customer_id) as subscribed_customers,
	   count(distinct case when datediff(month,subscription_month, month_of) = 1 and (open_subscriber = 1 or revenue > 0)  then customer_id end)*1.00/count(distinct customer_id) as M1_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 2 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M2_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 3 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M3_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 4 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M4_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 5 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M5_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 6 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M6_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 7 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M7_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 8 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M8_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 9 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M9_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 10 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M10_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 11 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M11_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 12 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M12_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 13 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M13_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 14 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M14_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 15 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M15_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 16 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M16_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 17 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M17_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 18 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M18_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 19 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M19_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 20 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M20_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 21 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M21_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 22 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M22_retention,
	   count(distinct case when datediff(month,subscription_month, month_of) = 23 and (open_subscriber = 1 or revenue > 0) then customer_id end)*1.00/count(distinct customer_id) as M23_retention
from                                                                                                             
dbo.intuit_case_study 
where channel = 'Inside Sales'
group by subscription_month, product, signup_type, channel
--order by 1 ;
)a
group by signup_type, product, channel 
order by 1,2 
