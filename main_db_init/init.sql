CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS REPORTING;



---DIM_DATE
create table curated.dim_date (
date_id int,
date date,
day int,
month int,
year int );

with recursive date(date) as (
select '01-01-1970'::date
union all
select date+1 from date where date<'01-01-2100'::date)
insert into curated.dim_date select to_char(date,'YYYYMMDD')::int,date,to_char(date,'DD')::int,to_char(date,'MM')::int,to_char(date,'YYYY')::int from date;

