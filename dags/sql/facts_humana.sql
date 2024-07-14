drop table if exists reporting.fact_encounters_humana;

create table reporting.fact_encounters_humana as 
select * 
from curated.fact_encounters
where payer='d47b3510-2895-3b70-9897-342d681c769d';

drop table if exists reporting.fact_procedures_humana;

create  table reporting.fact_procedures_humana as 
select * 
from curated.fact_procedures
where encounter in (select id from reporting.fact_encounters_humana); 



