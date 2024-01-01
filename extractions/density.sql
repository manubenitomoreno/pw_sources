 /*
ALTER TABLE sources.boundaries_geo DROP COLUMN sid;
ALTER TABLE sources.pois DROP COLUMN sid;

CREATE INDEX sid
  ON sources.boundaries_geo 
  USING GIST (geometry);
 
CREATE INDEX sid
  ON sources.pois 
  USING GIST (geometry);
 */



 
with
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
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT)
+ 
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing,
cast(data ->> 'Parcel Area' as float) as parcel_area,
geometry
from sources.pois where category = 'land use - general'
),

loaded_pois as (
select
pois.id poi_id,
pois.housing housing,
CEIL(pois.housing * sd.mean_household_size) population,
pois.parcel_area parcel_area,
rent_hog_gross,
mean_household_size,
p_households_1,
mean_age,
ceil(((pois.housing * sd.mean_household_size) * p_age_18)/100) population_age_18,
ceil(((pois.housing * sd.mean_household_size) * p_age_65)/100) population_age_65,
pois.geometry
from pois, sociodemo_geo sd where st_contains(sd.geometry,pois.geometry)),

nearest_poi as (
--select * from loaded_pois limit 1000
select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.alcala_network_relations anr where relation_kind = 'nearest_poi'),

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '900'),',') as ego_900
from networks.alcala_network_paths where relation_kind = 'ego_graphs'),

pois_table as (
select l.*,n.node_id,e.ego_900 from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id left join ego_graphs e on n.node_id = e.node_id
),

NodePopulations AS (
    SELECT cast(node_id as integer) node_id,
    SUM(population) as total_population,
    AVG(parcel_area) as mean_parcel_area,
    SUM(population_age_18) as total_population_age_18,
    SUM(population_age_65) as total_population_age_65,
    SUM(housing) as total_housing,
    SUM(rent_hog_gross * housing) / nullif(sum(housing),0) as mean_rent_hog_gross,
    SUM(mean_household_size * housing) / nullif(sum(housing),0) as mean_mean_household_size,
    SUM(mean_age * population) / nullif(sum(population),0) as mean_mean_age
    FROM pois_table
    GROUP BY node_id
),
UnnestedEgo AS (
    SELECT poi_id, cast(UNNEST(ego_900) as integer) as node_id
    FROM pois_table
),
metrics_on_ego as (
SELECT UnnestedEgo.poi_id,
SUM(total_population) as total_population,
AVG(mean_parcel_area) as mean_parcel_area,
SUM(total_population_age_18) as total_population_age_18,
SUM(total_population_age_65) as total_population_age_65,
SUM(total_housing) as total_housing,
SUM(mean_rent_hog_gross * total_housing) / nullif(sum(total_housing),0) as mean_rent_hog_gross,
SUM(mean_mean_household_size * total_housing) / nullif(sum(total_housing),0) as mean_mean_household_size,
SUM(mean_mean_age * total_population) / nullif(sum(total_population),0) as mean_mean_age
FROM UnnestedEgo
JOIN NodePopulations ON UnnestedEgo.node_id = NodePopulations.node_id 
GROUP BY UnnestedEgo.poi_id),

reloaded_pois as (select m.*,l.population local_population, l.geometry from metrics_on_ego m join loaded_pois l on m.poi_id = l.poi_id ),

transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone'),

final_pois as (
select g.id tz_id,p.* from reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

--select * from final_pois

zones_aggregated as (
select
g.id tz_id,
ROUND(cast(AVG(total_population) as NUMERIC),0) as total_population,
ROUND(cast(AVG(mean_parcel_area) as NUMERIC),0) as mean_parcel_area,
ROUND(cast(AVG(total_population_age_18) as NUMERIC),0) as total_population_age_18,
ROUND(cast(AVG(total_population_age_65) as NUMERIC),0) as total_population_age_65,
ROUND(cast(AVG(total_housing) as NUMERIC),0) as total_housing,
ROUND(cast(SUM(mean_rent_hog_gross * total_housing) / nullif(sum(total_housing),0)  as NUMERIC),0) as mean_rent_hog_gross,
ROUND(cast(SUM(mean_mean_household_size * total_housing) / nullif(sum(total_housing),0) as NUMERIC),2) as mean_mean_household_size,
ROUND(cast(SUM(mean_mean_age * total_population) / nullif(sum(total_population),0) as NUMERIC),2) as mean_mean_age
from reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
group by g.id)

select z.*,zt.geometry from zones_aggregated z join transportation_geo zt on z.tz_id = zt.id
