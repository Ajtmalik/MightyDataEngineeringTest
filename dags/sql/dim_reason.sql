drop table if exists curated.dim_reason;

create table curated.dim_reason as 
select reasoncode,reasondescription from staging.procedures
union distinct
select reasoncode,reasondescription from staging.encounters; 
