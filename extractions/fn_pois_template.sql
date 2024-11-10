
with

transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone'),

sociodemo as
(
select
split_part(id,'-',1) boundary_id,
nullif(greatest(cast(data ->> 'population' as float),0),0) as population,
nullif(greatest(cast(data ->> 'mean_household_size' as float ),0),0) as mean_household_size
from sources.boundaries_data bd where category = 'sociodemographic'),

census_geo as (
select
split_part(id,'-',3) boundary_id,
geometry
from sources.boundaries_geo
where sources.boundaries_geo.category = 'boundary-census_tract'
and split_part(id,'-',3) in (select boundary_id from sociodemo)),

sociodemo_geo as (
select
sociodemo.boundary_id census_id,
population,
mean_household_size,
census_geo.geometry
from sociodemo join census_geo on sociodemo.boundary_id = census_geo.boundary_id),

pois as (
select id poi_id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT)
+ 
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing,
geometry
from sources.pois where category = 'land use - general'
),

nearest_poi as (
select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),

loaded_pois as (

select n.node_id,c.* --,c.housing,st_astext(c.geometry) geometry
from pois c
left join nearest_poi n on c.poi_id = n.poi_id
),
reloaded_pois as (
select pois.*,
CEIL(housing * sd.mean_household_size) local_population
FROM loaded_pois pois
JOIN sociodemo_geo sd 
ON ST_Contains(sd.geometry, pois.geometry)
),

final_pois as (
select g.id tz_id, p.poi_id, node_id,local_population, housing, st_astext(p.geometry) geometry from reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
)

select * from final_pois