drop table if exists curated.dim_patient;

create table curated.dim_patient as
select distinct id,birthdate,deathdate,prefix,first,last,suffix,maiden,marital,race,ethnicity,gender,birthplace,address,city,state,county,zip,lat,lon
from staging.patients; 
