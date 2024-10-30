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

--delete from sources.pois where category = 'land use - general'


--delete from sources.boundaries_geo

	
 
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
--cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) as housing_sfr,
--cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing_ch,
cast(data ->> 'Destinations - Healthcare - Multiple - Area' as FLOAT) +
cast(data ->> 'Destinations - Healthcare - Public - Area' as FLOAT) as care,
cast(data ->> 'Destinations - Educational - Basic - Area' as FLOAT) as school,
cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Area' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Cultural - Area' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Shows - Area' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Shows - Area' as FLOAT) as leisure,
cast(data ->> 'Destinations - Retail - Mall - Area' as FLOAT) + 
cast(data ->> 'Destinations - Retail - Market or Supermarket - Area' as FLOAT) +
cast(data ->> 'Destinations - Retail - Stand Alone Retail - Area' as FLOAT) as shopping,
cast(data ->> 'Destinations - Sport - Multiple - Area' as FLOAT) as sport,

cast(data ->> 'Parcel Area' as float) as parcel_area,
cast(data ->> 'Parcel Built Area' as float) as parcel_built_area,
cast(data ->> 'Parcel Built Area Above Ground' as float) as parcel_built_ag,
	
geometry
from sources.pois where category = 'land use - general'
),

loaded_pois as (
select
sd.boundary_id,
pois.id poi_id,
coalesce(NULLIF(pois.housing,'NaN'),0) housing,
coalesce(CEIL(NULLIF(pois.housing,'NaN') * sd.mean_household_size),0) population,
coalesce(NULLIF(pois.housing_sfr,'NaN'),0) housing_sfr,
coalesce(NULLIF(pois.housing_ch,'NaN'),0) housing_ch,
coalesce(NULLIF(pois.care_multiple,'NaN'),0) care_multiple,
coalesce(NULLIF(pois.care_public,'NaN'),0) care_public,
coalesce(NULLIF(pois.edu_basic,'NaN'),0) edu_basic,
coalesce(NULLIF(pois.leisure_bar,'NaN'),0) leisure_bar,
coalesce(NULLIF(pois.leisure_cultural,'NaN'),0) leisure_cultural,
coalesce(NULLIF(pois.leisure_show,'NaN'),0) leisure_show,
coalesce(NULLIF(pois.leisure_sports,'NaN'),0) leisure_sports,
coalesce(NULLIF(pois.retail_mall,'NaN'),0) retail_mall,
coalesce(NULLIF(pois.retail_market,'NaN'),0) retail_market,
coalesce(NULLIF(pois.retail_alone,'NaN'),0) retail_alone,
coalesce(NULLIF(pois.sport_area,'NaN'),0) sport_area,

pois.parcel_area parcel_area,
pois.parcel_built_area parcel_built_area,
pois.parcel_built_ag parcel_built_ag,
rent_hog_gross,
mean_household_size,
p_households_1,
mean_age,
coalesce(ceil(((NULLIF(pois.housing,'NaN') * sd.mean_household_size) * p_age_18)/100),0) population_age_18,
coalesce(ceil(((NULLIF(pois.housing,'NaN') * sd.mean_household_size) * p_age_65)/100),0) population_age_65,
pois.geometry
from pois, sociodemo_geo sd where st_contains(sd.geometry,pois.geometry)),

pois_all_geo as (	
select
	geo.id as tz_id,
	left(p.boundary_id,2) as province_id,
	left(p.boundary_id,5) as municipality_id, 
	left(p.boundary_id,7) as district_id,
	p.boundary_id as census_id,
	p.*
	from loaded_pois p, transportation_geo geo
	where st_contains(geo.geometry,p.geometry)	
),

--select * from pois_all_geo
final as (	
select
	census_id boundary_id,
'census_tract' boundary_type,
sum(housing) housing,
sum(population) population,
	
sum(housing_sfr) housing_sfr,
sum(housing_ch) housing_ch,
sum(care_multiple) care_multiple,
sum(care_public) care_public,
sum(edu_basic) edu_basic,
sum(leisure_bar) leisure_bar,
sum(leisure_cultural) leisure_cultural,
sum(leisure_show) leisure_show,
sum(leisure_sports) leisure_sports,
sum(retail_mall) retail_mall,
sum(retail_market) retail_market,
sum(retail_alone) retail_alone,
sum(sport_area) sport_area,

sum(parcel_area) parcel_area,
sum(parcel_built_area) parcel_built_area,
sum(parcel_built_ag) parcel_built_ag

from pois_all_geo
group by census_id

UNION ALL

select
	district_id boundary_id,
'district' boundary_type,
sum(housing) housing,
sum(population) population,

sum(housing_sfr) housing_sfr,
sum(housing_ch) housing_ch,
sum(care_multiple) care_multiple,
sum(care_public) care_public,
sum(edu_basic) edu_basic,
sum(leisure_bar) leisure_bar,
sum(leisure_cultural) leisure_cultural,
sum(leisure_show) leisure_show,
sum(leisure_sports) leisure_sports,
sum(retail_mall) retail_mall,
sum(retail_market) retail_market,
sum(retail_alone) retail_alone,
sum(sport_area) sport_area,
	
sum(parcel_area) parcel_area,
sum(parcel_built_area) parcel_built_area,
sum(parcel_built_ag) parcel_built_ag
from pois_all_geo
group by district_id

UNION ALL

select
	municipality_id boundary_id,
'municipality' boundary_type,
sum(housing) housing,
sum(population) population,
	
sum(housing_sfr) housing_sfr,
sum(housing_ch) housing_ch,
sum(care_multiple) care_multiple,
sum(care_public) care_public,
sum(edu_basic) edu_basic,
sum(leisure_bar) leisure_bar,
sum(leisure_cultural) leisure_cultural,
sum(leisure_show) leisure_show,
sum(leisure_sports) leisure_sports,
sum(retail_mall) retail_mall,
sum(retail_market) retail_market,
sum(retail_alone) retail_alone,
sum(sport_area) sport_area,
	
sum(parcel_area) parcel_area,
sum(parcel_built_area) parcel_built_area,
sum(parcel_built_ag) parcel_built_ag
from pois_all_geo
group by municipality_id

UNION ALL

select
	tz_id boundary_id,
'tz' boundary_type,
sum(housing) housing,
sum(population) population,
	
sum(housing_sfr) housing_sfr,
sum(housing_ch) housing_ch,
sum(care_multiple) care_multiple,
sum(care_public) care_public,
sum(edu_basic) edu_basic,
sum(leisure_bar) leisure_bar,
sum(leisure_cultural) leisure_cultural,
sum(leisure_show) leisure_show,
sum(leisure_sports) leisure_sports,
sum(retail_mall) retail_mall,
sum(retail_market) retail_market,
sum(retail_alone) retail_alone,
sum(sport_area) sport_area,
	
sum(parcel_area) parcel_area,
sum(parcel_built_area) parcel_built_area,
sum(parcel_built_ag) parcel_built_ag
from pois_all_geo
group by tz_id

UNION ALL 
	
select
	province_id boundary_id,
'province' boundary_type,
sum(housing) housing,
sum(population) population,
	
sum(housing_sfr) housing_sfr,
sum(housing_ch) housing_ch,
sum(care_multiple) care_multiple,
sum(care_public) care_public,
sum(edu_basic) edu_basic,
sum(leisure_bar) leisure_bar,
sum(leisure_cultural) leisure_cultural,
sum(leisure_show) leisure_show,
sum(leisure_sports) leisure_sports, -- no entra, no esta bien
sum(retail_mall) retail_mall,
sum(retail_market) retail_market,
sum(retail_alone) retail_alone,
sum(sport_area) sport_area,
	
sum(parcel_area) parcel_area,
sum(parcel_built_area) parcel_built_area,
sum(parcel_built_ag) parcel_built_ag
from pois_all_geo
group by province_id)


select f.*,ST_AsEWKT(t.geometry) as geometry from final f left join transportation_geo t on f.boundary_id = t.id where boundary_type = 'tz' 


/*
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
