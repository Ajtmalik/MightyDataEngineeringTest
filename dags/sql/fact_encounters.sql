drop table if exists curated.fact_encounters;

create table curated.fact_encounters as 
select distinct id,start,stop,patient,organization,payer,encounterclass,code,base_encounter_cost,total_claim_cost,payer_coverage,reasoncode
from staging.encounters; 