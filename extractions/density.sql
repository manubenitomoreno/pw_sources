
--ALTER TABLE sources.boundaries_geo DROP COLUMN sid;
--ALTER TABLE sources.pois DROP COLUMN sid;
/*
CREATE INDEX sid
  ON sources.boundaries_geo 
  USING GIST (geometry);
 
CREATE INDEX sid
  ON sources.pois 
  USING GIST (geometry);

DROP TABLE transportation_geo;
DROP TABLE urban_geo;
DROP TABLE urban_area;
DROP TABLE sociodemo;
DROP TABLE geo;
DROP TABLE pois;
DROP TABLE sociodemo_geo;
*/
EXPLAIN ANALYZE

CREATE TEMP TABLE transportation_geo as (select bg.id transportation_id, bg.geometry tz_geometry from sources.boundaries_geo bg where category = 'boundary-transportation_zone');

CREATE TEMP TABLE urban_geo as (select ao.id urban_id, ao.geometry urban_geometry from sources.aois ao where category = 'land use - urban area');

CREATE TEMP TABLE urban_area AS (SELECT t.transportation_id, 
       (COALESCE (SUM (ST_AREA (
            ST_INTERSECTION (
                t.tz_geometry,
                u.urban_geometry
            )
        )
    ),0))/10000 AS urban_area_ha, t.tz_geometry tz_geometry
FROM transportation_geo t LEFT JOIN urban_geo u
ON ST_INTERSECTS (t.tz_geometry, u.urban_geometry) 
GROUP BY t.transportation_id, t.tz_geometry);

CREATE TEMP TABLE
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

from sources.boundaries_data bd where category = 'sociodemographic');

CREATE TEMP TABLE geo as (
select
split_part(id,'-',3) boundary_id,
geometry
from sources.boundaries_geo
where sources.boundaries_geo.category = 'boundary-census_tract'
and split_part(id,'-',3) in (select boundary_id from sociodemo));

CREATE TEMP TABLE sociodemo_geo as (
select
sociodemo.boundary_id,population,rent_hog_gross,
mean_household_size,
p_households_1,
mean_age,
p_age_18,
p_age_65,
p_national,
geo.geometry
from sociodemo join geo on sociodemo.boundary_id = geo.boundary_id);

CREATE TEMP TABLE pois as (
select id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) as housing_sfr_number,
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing_ch_number,
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) +
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing_number,

cast(data ->> 'Origins - Housing - Single Family Residence - Area' as FLOAT) as housing_sfr_area,
cast(data ->> 'Origins - Housing - Collective Housing - Area' as FLOAT) as housing_ch_area,
cast(data ->> 'Origins - Housing - Single Family Residence - Area' as FLOAT) +
cast(data ->> 'Origins - Housing - Collective Housing - Area' as FLOAT) as housing_area,
cast(data ->> 'Destinations - Educational - Basic - Area' as FLOAT) as educational_basic_area,
cast(data ->> 'Destinations - Educational - Superior - Area' as FLOAT) as educational_superior_area,
cast(data ->> 'Destinations - Healthcare - Multiple - Area' as FLOAT) as healthcare_multiple_area,
cast(data ->> 'Destinations - Healthcare - Public - Area' as FLOAT) as healthcare_public_area,
cast(data ->> 'Destinations - Leisure - Cultural - Area' as FLOAT) as leisure_cultural_area,
cast(data ->> 'Destinations - Leisure - Sport - Area' as FLOAT) as leisure_sport_area,
cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Area' as FLOAT) as leisure_bar_area,
cast(data ->> 'Destinations - Leisure - Shows - Area' as FLOAT) as leisure_shows_area,
cast(data ->> 'Destinations - Public - Multiple - Area' as FLOAT) as public_multiple_area,
cast(data ->> 'Destinations - Retail - Mall - Area' as FLOAT) as retail_mall_area,
cast(data ->> 'Destinations - Retail - Market or Supermarket - Area' as FLOAT) as retail_market_area,
cast(data ->> 'Destinations - Retail - Stand Alone Retail - Area' as FLOAT) as retail_stand_area,
cast(data ->> 'Destinations - Sport - Multiple - Area' as FLOAT) as sport_multiple_area,
cast(data ->> 'Other - Storage - Area' as FLOAT) as other_storage_area,
cast(data ->> 'Other - Parking - Area' as FLOAT) as other_parking_area,
cast(data ->> 'Other - Industrial - Area' as FLOAT) as other_industrial_area,
cast(data ->> 'Other - Office - Area' as FLOAT) as other_office_area,
cast(data ->> 'Parcel Area' as float) as parcel_area,
cast(data ->> 'Parcel Built Area' as float) as parcel_built_area,
cast(data ->> 'Parcel Built Area Above Ground' as float) as parcel_built_area_ag,
cast(data ->> 'Parcel Built Area Under Ground' as float) as parcel_built_area_ug,
cast(data ->> 'Parcel Built Area Under Cover' as float) as parcel_built_area_uc,
cast(data ->> 'Parcel Area - mean' as float) as parcel_area_mean,
cast(data ->> 'Parcel Built Area - mean' as float) as parcel_built_area_mean,
geometry
from sources.pois where category = 'land use - general'
);

CREATE TEMP TABLE loaded_pois as (
select
pois.id poi_id,
pois.housing_number housing,
CEIL(pois.housing_number * sd.mean_household_size) population,
rent_hog_gross,
mean_household_size,
p_households_1,
mean_age,
ceil(((pois.housing_number * sd.mean_household_size) * p_age_18)/100) population_age_18,
ceil(((pois.housing_number * sd.mean_household_size) * p_age_65)/100) population_age_65,
housing_sfr_number,housing_ch_number, housing_number,
housing_sfr_area, housing_ch_area, housing_area,
educational_basic_area, educational_superior_area,
healthcare_multiple_area, healthcare_public_area,
leisure_cultural_area, leisure_sport_area, leisure_bar_area, leisure_shows_area,
public_multiple_area,
retail_mall_area, retail_market_area, retail_stand_area,
sport_multiple_area,
other_storage_area, other_parking_area, other_industrial_area, other_office_area,
parcel_area, parcel_built_area, parcel_built_area_ag, parcel_built_area_ug, parcel_built_area_uc, parcel_area_mean, parcel_built_area_mean,
pois.geometry
from pois, sociodemo_geo sd where st_contains(sd.geometry,pois.geometry));

select * from loaded_pois

CREATE TEMP TABLE zones_aggregated as (
select
g.transportation_id transportation_id,
ROUND(cast(SUM(population) as NUMERIC),0) as population,
ROUND(cast(SUM(population_age_18) as NUMERIC),0) as population_age_18,
ROUND(cast(SUM(population_age_65) as NUMERIC),0) as population_age_65,
ROUND(cast(SUM(rent_hog_gross * housing_number) / nullif(sum(housing_number),0)  as NUMERIC),0) as rent_hog_gross,
ROUND(cast(SUM(mean_household_size * housing_number) / nullif(sum(housing_number),0) as NUMERIC),2) as mean_household_size,
ROUND(cast(SUM(mean_age * population) / nullif(sum(population),0) as NUMERIC),2) as mean_age
ROUND(cast(SUM(housing_number) as NUMERIC),0) as housing_number,
ROUND(cast(SUM(housing_sfr_number) as NUMERIC),0) as housing_sfr_number,
ROUND(cast(SUM(housing_ch_number) as NUMERIC),0) as housing_ch_number,
ROUND(cast(SUM(housing_area) as NUMERIC),0) as housing_area,
ROUND(cast(SUM(housing_sfr_area) as NUMERIC),0) as housing_sfr_area,
ROUND(cast(SUM(housing_ch_area) as NUMERIC),0) as housing_ch_area,
ROUND(cast(SUM(educational_basic_area) as NUMERIC),0) as educational_basic_area,
ROUND(cast(SUM(educational_superior_area) as NUMERIC),0) as educational_superior_area,
ROUND(cast(SUM(healthcare_multiple_area) as NUMERIC),0) as healthcare_multiple_area,
ROUND(cast(SUM(healthcare_public_area) as NUMERIC),0) as healthcare_public_area,
ROUND(cast(SUM(leisure_cultural_area) as NUMERIC),0) as leisure_cultural_area,
ROUND(cast(SUM(leisure_sport_area) as NUMERIC),0) as leisure_sport_area,
ROUND(cast(SUM(leisure_bar_area) as NUMERIC),0) as leisure_bar_area,
ROUND(cast(SUM(leisure_shows_area) as NUMERIC),0) as leisure_shows_area,
ROUND(cast(SUM(public_multiple_area) as NUMERIC),0) as public_multiple_area,
ROUND(cast(SUM(retail_mall_area) as NUMERIC),0) as retail_mall_area,
ROUND(cast(SUM(retail_market_area) as NUMERIC),0) as retail_market_area,
ROUND(cast(SUM(retail_stand_area) as NUMERIC),0) as retail_stand_area,
ROUND(cast(SUM(sport_multiple_area) as NUMERIC),0) as sport_multiple_area,
ROUND(cast(SUM(sport_multiple_area) as NUMERIC),0) as sport_multiple_area,
other_storage_area, other_parking_area, other_industrial_area, other_office_area,
parcel_area, parcel_built_area, parcel_built_area_ag, parcel_built_area_ug, parcel_built_area_uc, parcel_area_mean, parcel_built_area_mean,

urban_area_ha,
g.geometry,
from loaded_pois p, urban_area g where st_contains(g.geometry,p.geometry)
group by g.id);

SELECT * FROM zones_aggregated


--select * from loaded_pois
/*
,

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
*/