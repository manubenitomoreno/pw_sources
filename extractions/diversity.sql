with 
pois_trips as (
select id poi_id,node_id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) 
+ 
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing,
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) 
+ 
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) * 3.0625 as origins,
cast(data ->> 'Destinations - Retail - Mall - Area' as FLOAT) * 0.165 +
cast(data ->> 'Destinations - Retail - Stand Alone Retail - Area' as FLOAT) * 0.165 +
cast(data ->> 'Destinations - Retail - Market or Supermarket - Area' as FLOAT) * 0.165 as destinations_retail,
cast(data ->> 'Destinations - Sport - Multiple - Area' as FLOAT) * 0.017  +
cast(data ->> 'Destinations - Leisure - Sport - Area' as FLOAT) * 0.017 as destinations_sport,
cast(data ->> 'Destinations - Leisure - Shows - Area' as FLOAT) * 0.061 as destinations_shows,
cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Area' as FLOAT) * 0.351 as destinations_hospitality,
cast(data ->> 'Destinations - Leisure - Cultural - Area' as FLOAT) * 0.024 as destinations_cultural,
cast(data ->> 'Destinations - Healthcare - Multiple - Area' as FLOAT) * 0.025 as destinations_healthcare,
cast(data ->> 'Destinations - Educational - Basic - Area' as FLOAT) * 0.024 +
cast(data ->> 'Destinations - Educational - Superior - Area' as FLOAT) * 0.024 as destinations_education,
geometry
from sources.pois p
join (select split_part(relation_id,'|',1) poi_id,split_part(relation_id,'|',2) node_id from networks.alcala_network_relations) anr 
on p.id = anr.poi_id
where p.category = 'land use - general'),

ego_graphs as 
(select split_part(relation_id,'|',2) node_id,
string_to_array(trim(both '[]' from data ->> '900'),',') as ego_900
from networks.alcala_network_paths where relation_kind = 'ego_graphs'),

unnested_ego AS (
    SELECT node_id, cast(UNNEST(ego_900) as integer) as node_id_ego
    FROM ego_graphs
),

nodes_trips as (
select node_id,
sum(origins) as origins,
sum(destinations_retail) +
sum(destinations_sport) +
sum(destinations_shows) +
sum(destinations_hospitality) +
sum(destinations_cultural) +
sum(destinations_healthcare) +
sum(destinations_education) as destinations
from pois_trips group by node_id
),

grouped_trips as (
select u.node_id,sum(origins) origins, sum(destinations) destinations 
from unnested_ego u left join nodes_trips t on u.node_id_ego = cast(t.node_id as integer) group by u.node_id),

walkable_trips as (
select node_id, origins, destinations, 
case 
when origins < destinations then origins
when destinations < origins then destinations
when origins = destinations then origins
end as walkable_trips,
case 
when origins < destinations then destinations - origins
when origins > destinations then (origins - destinations)*(-1)
end as unpaired_trips
from grouped_trips),

loaded_pois as (
select
poi_id,p.housing,w.node_id, w.origins,w.destinations,walkable_trips,unpaired_trips,p.geometry
from pois_trips p join walkable_trips w on w.node_id = p.node_id
),

transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone'),

final_pois as (
select g.id tz_id,p.* from loaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

aggregated_tz as (
select 
tz_id,
ROUND(cast(SUM(walkable_trips * housing) / nullif(sum(housing),0) as NUMERIC),0) as mean_walkable_trips,
ROUND(cast(SUM(unpaired_trips * housing) / nullif(sum(housing),0) as NUMERIC),0) as mean_unpaired_trips
from final_pois
group by tz_id)

select a.*, g.geometry from aggregated_tz a join transportation_geo g on a.tz_id = g.id

