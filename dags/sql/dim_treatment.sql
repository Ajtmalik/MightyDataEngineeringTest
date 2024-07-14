drop table if exists curated.dim_treatment;

create table curated.dim_treatment as 
select code,max(description) "description" from
(select code,max(description) "description" from staging.procedures group by 1
union distinct
select code,max(description) "description" from staging.encounters group by 1 
) temp group by 1;
