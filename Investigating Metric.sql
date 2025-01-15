use investigating_metric_spike;
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

select * from users;
select distinct(event_name) from events;

SELECT 
    EXTRACT(WEEK FROM occured_at_dt) AS week_day,
    COUNT(user_id) AS active_users
FROM
    events
GROUP BY week_day;

-- CREATE VIEW new_users AS
    SELECT 
        STR_TO_DATE(created_id_dt, '%Y-%m-%d') AS acc_created,
        COUNT(DISTINCT (a.user_id)) AS no_of_new_users
    FROM
        users a
            LEFT OUTER JOIN
        events b ON a.user_id = b.user_id
    WHERE
        b.event_name LIKE '%signup%'
    GROUP BY STR_TO_DATE(created_id_dt, '%Y-%m-%d');


select acc_created, no_of_new_users, sum(no_of_new_users) over (order by acc_created) as cummulative_new_users from new_users;

SELECT
    tot_users,
    100 * ((tot_users - LAG(tot_users, 1, 0) OVER (ORDER BY growth_by_week)) / LAG(tot_users, 1, 0) OVER (ORDER BY growth_by_week)) AS percent_growth
FROM
    (SELECT
        EXTRACT(WEEK FROM acc_created) AS growth_by_week,
        SUM(no_of_new_users) AS tot_users
    FROM
        new_users
    GROUP BY
        growth_by_week
    ) a;
    







WITH cohort_week AS (
    SELECT
        user_id,
        WEEK(created_id_dt) AS joining_week,
        YEAR(created_id_dt) AS joining_year,
        created_id_dt
    FROM users
    ORDER BY 1
),
user_activities AS (
    SELECT
        e.user_id AS id,
        occured_at_dt,
        created_id_dt,
        DATEDIFF(occured_at_dt, created_id_dt),
        CEIL(DATEDIFF(occured_at_dt, created_id_dt) / 7) AS active_diff_week,
        joining_week,
        joining_year
    FROM cohort_week c
    LEFT OUTER JOIN events e ON e.user_id = c.user_id
    WHERE event_type <> 'signup_flow'
    GROUP BY id, active_diff_week
),
cohort_size AS (
    SELECT
        joining_year,
        joining_week,
        COUNT(user_id) AS num_users
    FROM cohort_week
    GROUP BY joining_year, joining_week
    ORDER BY joining_year, joining_week
),
retention_table AS (
    SELECT
        C.joining_year,
        C.joining_week,
        A.active_diff_week,
        COUNT(id) AS num_users
    FROM cohort_week C
    LEFT JOIN user_activities A ON A.id = C.user_id
    GROUP BY joining_year, joining_week, active_diff_week
)
SELECT
    joining_year,
    joining_week,  MAX(total_users) AS total_users,
    round(coalesce(MAX(CASE WHEN active_diff_week = 0 THEN percentage END),0), 2) AS week_0,
    round(coalesce(MAX(CASE WHEN active_diff_week = 1 THEN percentage END), 0), 2) AS week_1,
	round(coalesce(MAX(CASE WHEN active_diff_week = 2 THEN percentage END), 0), 2) AS week_2,
    round(coalesce(MAX(CASE WHEN active_diff_week = 3 THEN percentage END), 0), 2) AS week_3
FROM (
    SELECT
        R.joining_year,
        R.joining_week,
        S.num_users AS total_users,
        R.active_diff_week,
        R.num_users * 100 / S.num_users AS percentage
    FROM cohort_size S
    LEFT JOIN retention_table R ON R.joining_year = S.joining_year AND R.joining_week = S.joining_week
    WHERE R.active_diff_week < 4
) AS pivot_tbl
GROUP BY joining_year, joining_week
ORDER BY joining_year, joining_week;

SELECT 
    weekly,
    SUM(CASE WHEN device_type = 'Tablet' THEN device_count ELSE 0 END) AS Tablet,
    SUM(CASE WHEN device_type = 'Notebook' THEN device_count ELSE 0 END) AS Notebook,
    SUM(CASE WHEN device_type = 'Mobile' THEN device_count ELSE 0 END) AS Mobile,
    SUM(CASE WHEN device_type = 'Other' THEN device_count ELSE 0 END) AS Other,
    sum(device_count) as Total
FROM(
select week(occured_at_dt) as weekly, CASE 
            WHEN device IN ('ipad mini', 'nexus 7', 'samsung galaxy tablet') THEN 'Tablet'
            WHEN device IN ('dell inspiron notebook', 'macbook air', 'macbook pro', 'acer aspire notebook', 'asus chromebook', 'mac mini', 'hp pavilion desktop', 'acer aspire desktop') THEN 'Notebook'
            WHEN device IN ('iphone 5', 'iphone 4s', 'iphone 5s', 'nexus 5', 'samsung galaxy s4', 'htc one', 'amazon fire phone', 'nokia lumia 635') THEN 'Mobile'
            WHEN device IN ('windows surface', 'kindle fire', 'nexus 10', 'samsung galaxy note') THEN 'Tablet'
            ELSE 'Other'
        END AS device_type , count(user_id) as device_count from events
where event_name <> 'complete_signup'
group by weekly, device_type) pivot_tbl
group by weekly
order by weekly;

select week(occured_at_dt) as weekly ,device, count(user_id) from events
group by weekly;



-- WHERE EVENT_TYPE LIKE '%EMAIL%'

select count(distinct(user_id)), (select count(distinct(user_id)) from email_events)  from email_events
where action = 'email_open';

-- TOTAL EMAIL ENGAGEMENT
SELECT count(*) as total_engagement FROM EMAIL_EVENTS;

-- EMail clickthrough
select 100 * count(distinct (case when action = 'email_clickthrough' then user_id end))/count(distinct(user_id)) as clickthrough_rate
from email_events;

-- email_open
select 100 * count(distinct (case when action = 'email_open' then user_id end))/count(distinct(user_id)) as Unique_email_open 
from email_events;

