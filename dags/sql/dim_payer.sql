drop table if exists curated.dim_payer;

create table curated.dim_payer as
select distinct id,name,address,city,state_headquartered,zip,phone
from staging.payers; 
