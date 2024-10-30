
with

nearest_poi as (

select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '900'),',') as ego_900
from networks.amm_network_ego where relation_kind = 'ego_graphs'),

sociodemo as
(
select
split_part(id,'-',1) boundary_id,
nullif(cast(data ->> 'population' as float),-999) as population,
nullif(cast(data ->> 'rent_hog_gross' as float ),-999) as rent_hog_gross,
nullif(cast(data ->> 'mean_household_size' as float ),-999) as mean_household_size,
nullif(cast(data ->> 'p_households_1' as float ),-999) as p_households_1,
nullif(cast(data ->> 'mean_age' as float ),-999) as mean_age,
nullif(cast(data ->> 'p_age_18' as float ),-999) as p_age_18,
nullif(cast(data ->> 'p_age_65' as float ),-999) as p_age_65,
nullif(cast(data ->> 'p_national' as float ),-999) as p_national

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
from sociodemo join geo on sociodemo.boundary_id = geo.boundary_id),

pois as (
select id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) +
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing,	
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
pois.geometry
from pois, sociodemo_geo sd where st_contains(sd.geometry,pois.geometry)),

pois_table as (
select l.*,n.node_id, e.ego_900 from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id left join ego_graphs e on n.node_id = e.node_id
),

node_populations AS (
    SELECT cast(node_id as integer) node_id,
    SUM(population) as population,
	SUM(housing) as housing,
	SUM(rent_hog_gross * housing) / nullif(sum(housing),0) as rent_hog_gross,
	SUM(mean_household_size * housing) / nullif(sum(housing),0) as mean_household_size,
	SUM(mean_age * housing) / nullif(sum(housing),0) as mean_age,
	SUM(population_age_18)*100/NULLIF(SUM(population),0) as population_age_18,
	SUM(population_age_65)*100/NULLIF(SUM(population),0) as population_age_65,
    FROM pois_table
    GROUP BY node_id
),

unnested_ego AS (
    SELECT poi_id, cast(UNNEST(ego_900) as integer) as node_id
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
FROM unnested_ego
JOIN node_populations ON unnested_ego.node_id = node_populations.node_id 
GROUP BY unnested_ego.poi_id),

reloaded_pois as (select m.*,l.population local_population, l.geometry from metrics_on_ego m join loaded_pois l on m.poi_id = l.poi_id ),

final_pois as (
SELECT
g.id tz_id,
local_population,
population,
housing,
rent_hog_gross DEM_INCOME,
mean_household_size DEM_HOUSEHOLD_SIZE,
mean_age DEM_MEAN_AGE,
ROUND(CAST((p.population_age_18*100) / NULLIF(p.population,0) AS NUMERIC),2) DEM_POPULATION_18,
ROUND(CAST((p.population_age_65*100) / NULLIF(p.population,0) AS NUMERIC),2) DEM_POPULATION_65,
FROM reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

zones_aggregated as (
select
tz_id,
ROUND(cast(SUM(DEM_INCOME * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DEM_INCOME,
ROUND(cast(SUM(DEM_HOUSEHOLD_SIZE * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEM_HOUSEHOLD_SIZE,
ROUND(cast(SUM(DEM_MEAN_AGE * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEM_MEAN_AGE,
ROUND(cast(SUM(DEM_POPULATION_18 * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEM_POPULATION_18,
ROUND(cast(SUM(DEM_POPULATION_65 * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEM_POPULATION_65,

from final_pois
group by tz_id)

select z.*,zt.geometry
from zones_aggregated z join transportation_geo zt on z.tz_id = zt.id
--where dens_pop_total is not null
