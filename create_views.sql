-- View examples for Milestone 7
-- Note: the project id in the FROM clause is required when creating views
create view oscars.v_Highest_Nominated_Actors as
select entity, count(*) as nomination_count
from `cs327e-fa2019.oscars.Nomination_Events` 
where category like '%ACTOR%' 
group by entity
order by nomination_count desc
limit 30

create view oscars.v_Highest_Nominated_Actresses as
select entity, count(*) as nomination_count
from `cs327e-fa2019.oscars.Nomination_Events`  
where category like '%ACTRESS%' 
group by entity
order by nomination_count desc
limit 30

create view oscars.v_Highest_Nominated_Movies as
select entity, year, count(*) as nomination_count
from `cs327e-fa2019.oscars.Nomination_Events`  
where category in ('BEST MOTION PICTURE', 'BEST PICTURE', 'CINEMATOGRAPHY', 'WRITING (Story and Screenplay)', 
	                'ART DIRECTION', 'DIRECTING', 'FILM EDITING', 'MUSIC (Original Dramatic Score)', 'MUSIC (Original Song)', 
				     'SOUND', 'SOUND EFFECTS EDITING', 'VISUAL EFFECTS', 'MAKEUP') 
group by entity, year
order by nomination_count desc
limit 30

-- Querying the views: 
-- select * from oscars.v_Highest_Nominated_Actors
-- select * from oscars.v_Highest_Nominated_Actresses
-- select * from oscars.v_Highest_Nominated_Movies

