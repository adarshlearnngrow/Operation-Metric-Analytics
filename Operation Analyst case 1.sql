USE job;
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

alter table job_data add column ds_yymd date;
update job_data set ds_yymd = str_to_date(ds, '%m/%d/%Y');
alter table job_data drop column ds;
SELECT * FROM job_data;


-- Jobs Reviewed Over Time:
-- Objective: Calculate the number of jobs reviewed per hour for each day in November 2020.
-- Your Task: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.
-- select round(avg(time_spent_per_job),2) as hr_spent from (
SELECT 
    ds_yymd,
    ROUND(COUNT(job_id) / (SUM(time_spent) / 3600), 2) AS job_per_hour
FROM job_data
WHERE MONTH(ds_yymd) = 11 AND YEAR(ds_yymd) = 2020
GROUP BY ds_yymd
order by ds_yymd;



-- Throughput Analysis:
-- Objective: Calculate the 7-day rolling average of throughput (number of events per second).
-- Your Task: Write an SQL query to calculate the 7-day rolling average of throughput. Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.

   
 
select ds_yymd,
sum(cnt_job_id) over (order by ds_yymd rows between 6 Preceding  and current row)/
sum(tot_time_spent) over (order by ds_yymd rows between 6 Preceding  and current row) as rolling_avg,
cnt_job_id/tot_time_spent as daily_metric from
(select ds_yymd, count(job_id) as cnt_job_id, sum(time_spent)as tot_time_spent from job_data
group by ds_yymd) actual_data;
    


select language, (count(job_id)/(select count(*) from job_data)) * 100 as market_share from job_data
where ds_yymd between '2020-11-01' and '2020-11-30'
group by language
order by market_share desc;




WITH DuplicateCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_id) AS rn
    FROM job_data
)
SELECT *
FROM DuplicateCTE
WHERE rn > 1;
