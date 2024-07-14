drop table if exists curated.dim_organization;

create table curated.dim_organization as
select distinct id,name,address,city,state,zip,lat,lon
from staging.organizations; 
