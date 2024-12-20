--delete from networks.amm_network_nodes;
--delete from networks.amm_network_edges;
--delete from networks.amm_network_relations;
--delete from networks.amm_network_ego;

--select distinct(provider) from networks.amm_network_edges

with

nearest_poi as (

select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '1200'),',') as ego
from networks.amm_network_ego where relation_kind = 'ego_graphs'),

sociodemo as
(
select
split_part(id,'-',1) boundary_id,
nullif(greatest(cast(data ->> 'population' as float),0),0) as population,
nullif(greatest(cast(data ->> 'rent_hog_gross' as float ),0),0) as rent_hog_gross,
nullif(greatest(cast(data ->> 'mean_household_size' as float ),0),0) as mean_household_size,
nullif(greatest(cast(data ->> 'p_households_1' as float ),0),0) as p_households_1,
nullif(greatest(cast(data ->> 'mean_age' as float ),0),0) as mean_age,
nullif(greatest(cast(data ->> 'p_age_18' as float ),0),0) as p_age_18,
nullif(greatest(cast(data ->> 'p_age_65' as float ),0),0) as p_age_65,
nullif(greatest(cast(data ->> 'p_national' as float ),0),0) as p_national

from sources.boundaries_data bd where category = 'sociodemographic'),

geo as (
select
split_part(id,'-',3) boundary_id,
geometry
from sources.boundaries_geo
where sources.boundaries_geo.category = 'boundary-census_tract'
and split_part(id,'-',3) in (select boundary_id from sociodemo)),

transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone'),

sociodemo_geo as (
select
sociodemo.boundary_id,population,rent_hog_gross,
mean_household_size,
p_households_1,
mean_age,
p_age_18,
p_age_65,
p_national,
geo.geometry
from sociodemo join geo on sociodemo.boundary_id = geo.boundary_id where sociodemo.population > 0),

pois as (
select id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) +
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing,	
cast(data ->> 'Building Year Built - mean' as FLOAT) as building_age,	
cast(data ->> 'Parcel Built Area' as FLOAT) as built_area,	
geometry
from sources.pois where category = 'land use - general'
),

loaded_pois as (
select
sd.boundary_id,
pois.id poi_id,
coalesce(NULLIF(pois.housing,'NaN'),0) housing,
coalesce(CEIL(NULLIF(pois.housing,'NaN') * sd.mean_household_size),0) population,
rent_hog_gross,
mean_household_size,
coalesce(ceil((NULLIF(pois.housing,'NaN') * p_households_1)/100),0) households_1,
mean_age,
coalesce(ceil(((NULLIF(pois.housing,'NaN') * sd.mean_household_size) * p_age_18)/100),0) population_age_18,
coalesce(ceil(((NULLIF(pois.housing,'NaN') * sd.mean_household_size) * p_age_65)/100),0) population_age_65,
coalesce(ceil(((NULLIF(pois.housing,'NaN') * sd.mean_household_size) * p_national)/100),0) population_national,
NULLIF(pois.building_age,'NaN') building_age,
NULLIF(pois.built_area,'NaN') built_area,
pois.geometry
from pois, sociodemo_geo sd where st_contains(sd.geometry,pois.geometry)),

pois_table as (
select l.*,n.node_id, e.ego from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id left join ego_graphs e on n.node_id = e.node_id where building_age > 1700
)
,

node_populations AS (
    SELECT cast(node_id as integer) node_id,
    SUM(population) as population,
	SUM(housing) as housing,
	SUM(rent_hog_gross * housing) / nullif(sum(housing),0) as rent_hog_gross,
	SUM(mean_household_size * housing) / nullif(sum(housing),0) as mean_household_size,
	SUM(mean_age * housing) / nullif(sum(housing),0) as mean_age,
	SUM(population_age_18)*100/NULLIF(SUM(population),0) as population_age_18,
	SUM(population_age_65)*100/NULLIF(SUM(population),0) as population_age_65,
	SUM(building_age * built_area) / nullif(sum(built_area),0) building_age,
	SUM(built_area) built_area
    FROM pois_table
    GROUP BY node_id
),

unnested_ego AS (
    SELECT poi_id, cast(UNNEST(ego) as integer) as node_id
    FROM pois_table
),

metrics_on_ego as (
SELECT unnested_ego.poi_id,
SUM(population) population,
SUM(housing) housing,
SUM(rent_hog_gross * population) / nullif(sum(population),0) rent_hog_gross,
SUM(mean_household_size * population) / nullif(sum(population),0) mean_household_size,
SUM(mean_age * population) / nullif(sum(population),0) mean_age,
SUM(population_age_18) population_age_18,
SUM(population_age_65) population_age_65,
SUM(building_age * built_area) / nullif(sum(built_area),0) building_age
FROM unnested_ego
JOIN node_populations ON unnested_ego.node_id = node_populations.node_id 
GROUP BY unnested_ego.poi_id),

reloaded_pois as (select m.*,l.population local_population, l.geometry from metrics_on_ego m join loaded_pois l on m.poi_id = l.poi_id ),

final as (
SELECT
g.id tz_id,
poi_id,
local_population,
population,
housing,
rent_hog_gross DEM_INCOME,
mean_household_size DEM_HOUSEHOLD_SIZE,
mean_age DEM_MEAN_AGE,
ROUND(CAST((p.population_age_18*100) / NULLIF(p.population,0) AS NUMERIC),2) DEM_POPULATION_18,
ROUND(CAST((p.population_age_65*100) / NULLIF(p.population,0) AS NUMERIC),2) DEM_POPULATION_65,
building_age DES_BUILDING_AGE
FROM reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry))

select * from final 