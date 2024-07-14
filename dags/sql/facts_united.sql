drop table if exists reporting.fact_encounters_united;

create table reporting.fact_encounters_united as 
select * 
from curated.fact_encounters
where payer='5059a55e-5d6e-34d1-b6cb-d83d16e57bcf';

drop table if exists reporting.fact_procedures_united;

create  table reporting.fact_procedures_united as 
select * 
from curated.fact_procedures
where encounter in (select id from reporting.fact_encounters_united); 



