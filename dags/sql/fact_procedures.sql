drop table if exists curated.fact_procedures;

create table curated.fact_procedures as 
select distinct start,stop,patient,encounter,code,base_cost,reasoncode
from staging.procedures; 
