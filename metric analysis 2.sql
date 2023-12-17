use  user_engagement;
select * from events;
#weekly user engagement
SELECT 
    COUNT(event_name) AS engagement_cnt,
    WEEK(occurred_at) AS week_num
FROM
    events
WHERE
    event_type != 'signup_flow'
    group by week_num order by engagement_cnt desc;
    
    
    
    
    
 #alternate sol-1
 select *,
new_user_activated-lag(new_user_activated) over( order by year_,quarter_ ) as user_growth
from(select year(created_at) as year_,quarter(created_at) as quarter_,count(user_id) as new_user_activated 
from users 
where 
activated_at is not null and state= "active"
group by 1,2)a ;

 
#user growth for product monthly
with cte as(
SELECT 
    COUNT(user_id) AS users_created,
    quarter(created_at) as quarter_of_the_yr,
    monthname(created_at) AS month_of_the_yr
FROM
    users
    where state="active"
    group by quarter_of_the_yr,month_of_the_yr
)
select quarter_of_the_yr,month_of_the_yr,users_created,users_created-LAG(users_created) over() as usr_growth_metrics from cte;

# weekly retention cohort analysis:

select *,
timestampdiff(week,u.activated_at,e.occurred_at) as tymstamp from users u
join events e
on u.user_id=e.user_id
where e.event_name="login"
order by tymstamp;

select count(*) as activated_usrs_count , week(u.activated_at) as activated_week , year(u.activated_at) as yr_activated,
timestampdiff(week,u.activated_at,e.occurred_at) as tymstamp
from users u
join events e
on u.user_id=e.user_id
group by tymstamp,yr_activated, activated_week having tymstamp=1
order by activated_week
;
select * from events;

select a.activated_at,b.occurred_at,
count(distinct a.user_id) as cohort_retained,
timestampdiff(week,a.activated_at,b.occurred_at) as week_period
from users a 
join events b
on a.user_id=b.user_id
group by week_period,a.activated_at,b.occurred_at
order by week_period;

Select
week_period,
first_value(cohort_retained) over (order by week_period) as cohort_size,
cohort_retained,
cohort_retained / first_value(cohort_retained) over (order by week_period) as pct_retained 
From
(select
timestampdiff(week,a.activated_at,b.occurred_at) as week_period,
count(distinct a.user_id) as cohort_retained
From
(select user_id, activated_at
 from users where state='active') a
inner join
(select user_id,occurred_at from events )b
 on a.user_id=b.user_id
group by 1) c;

# weekly retention cohort analysis:
SELECT first AS "Week Numbers",
SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "Week 17", 
SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "Week 18",
SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "Week 19",
SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "Week 20",
SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "Week 21",
SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "Week 22",
SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "Week 23",
SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "Week 24",
SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "Week 25",
SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "Week 26",
SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "Week 27",
SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "Week 28",
SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "Week 29",
SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "Week 30",
SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "Week 31",
SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "Week 32",
SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "Week 33",
SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "Week 34",
SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "Week 35"
from (
SELECT m.user_id, m.login_week, n.first, m.login_week - first as week_number FROM 
(SELECT  user_id, week(occurred_at) as login_week FROM events GROUP BY 1, 2) m, 
(SELECT  user_id,  MIN(week(occurred_at)) AS first from events group BY 1) n 
WHERE m.user_id = n.user_id ) sub 
GROUP BY first
ORDER BY first;  

select device,count(device) as device_used_per_week, count(distinct user_id) as engagement_per_user , week(occurred_at) as Week_of_year from events 
where event_type !="signup_flow" and event_name= "login"
group by Week_of_year,device
order by engagement_per_user desc;

#weekly engagement per device
with cte as(
select device,count(device) as device_used_per_week, count(distinct user_id) as engagement_per_user , 
week(occurred_at) as Week_of_year from events 
where event_type !="signup_flow" and event_name= "login"
group by Week_of_year,device
order by engagement_per_user desc
)
select device, avg(device_used_per_week) as avg_device_usage_weekly , 
avg(engagement_per_user) as avg_engagement_user_weekly from cte
group by device;

# Other way using sub query

Select
device_name,
avg(num_users_using_device) as avg_weekly_users,
avg(times_device_use_current_week) as avg_times_used_weekly
From
(select week(occurred_at) as week,
device as device_name ,
count(distinct user_id) as num_users_using_device,
count(device) as times_device_use_current_week 
from events
where event_name="login"
group by 1,2
order by 1) a
group by 1;

#email engagement metrics:
Select
week,
num_users,
time_weekly_digest_sent,
time_weekly_digest_sent-lag(time_weekly_digest_sent) over(order by week) as time_weekly_digest_sent_growth,
time_email_open,time_email_open-lag(time_email_open) over(order by week) as time_email_open_growth,
time_email_clickthrough,time_email_clickthrough-lag(time_email_clickthrough) over(order by week) as time_email_clickthrough_growth
From
(select week(occurred_at)as week,
count(distinct user_id) as num_users,
 sum(if(action='sent_weekly_digest',1,0)) as time_weekly_digest_sent,
sum(if(action='email_open',1,0)) as time_email_open,
sum(if(action='email_clickthrough',1,0)) as time_email_clickthrough 
from email_events  
group by 1 
order by 1) a;


