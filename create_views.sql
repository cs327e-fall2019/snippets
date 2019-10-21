-- View examples for Milestone 7
-- Note: the project id in the FROM clause is required when creating views
create view oscars.v_Actor_Nomination_Counts as
select entity, count(*) as nomination_count
from `cs327e-fa2019.oscars.Nomination_Events` 
where category like '%ACTOR%' 
group by entity
order by nomination_count desc
limit 30

create view oscars.v_Actress_Nomination_Counts as
select entity, count(*) as nomination_count
from `cs327e-fa2019.oscars.Nomination_Events`  
where category like '%ACTRESS%' 
group by entity
order by nomination_count desc
limit 30

create view oscars.v_Director_Nomination_Counts as
select entity, count(*) as nomination_count
from `cs327e-fa2019.oscars.Nomination_Events`  
where category like '%DIRECTOR%' 
group by entity
order by nomination_count desc
limit 30

-- Query the views: 
-- select * from oscars.v_Actor_Nomination_Counts
-- select * from oscars.v_Actress_Nomination_Counts
-- select * from oscars.v_Director_Nomination_Counts

