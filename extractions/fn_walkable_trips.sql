--DELETE FROM sources.pois
WITH

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
nullif(cast(data ->> 'mean_household_size' as float ),-999) as mean_household_size
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

transportation_geo AS (
  SELECT * 
  FROM sources.boundaries_geo 
  WHERE category = 'boundary-transportation_zone'
),

pois AS (
  SELECT 
    id, 
    COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Other - Number' AS FLOAT),'NaN'), 0) AS housing_other_number,
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Single Family Residence - Number' AS FLOAT),'NaN'), 0) AS housing_sfr_number,
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Collective Housing - Number' AS FLOAT),'NaN'), 0) AS housing_ch_number,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Healthcare - Multiple - Number' AS FLOAT),'NaN'), 0) AS care_multiple_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Healthcare - Other - Number' AS FLOAT),'NaN'), 0) AS care_other_number,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Healthcare - Public - Number' AS FLOAT),'NaN'), 0) AS care_public_number,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Educational - Superior - Number' AS FLOAT),'NaN'), 0) AS school_superior_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Educational - Other - Number' AS FLOAT),'NaN'), 0) AS school_other_number,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Educational - Basic - Number' AS FLOAT),'NaN'), 0) AS school_basic_number,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Number' AS FLOAT),'NaN'), 0) AS leisure_bar_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Cultural - Number' AS FLOAT),'NaN'), 0) AS leisure_cultural_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Other - Number' AS FLOAT),'NaN'), 0) AS leisure_other_number,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Shows - Number' AS FLOAT),'NaN'), 0) AS leisure_shows_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Mall - Number' AS FLOAT),'NaN'), 0) AS shopping_mall_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Other - Number' AS FLOAT),'NaN'), 0) AS shopping_other_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Market or Supermarket - Number' AS FLOAT),'NaN'), 0) AS shopping_market_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Stand Alone Retail - Number' AS FLOAT),'Nan'), 0) AS shopping_alone_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Sport - Other - Number' AS FLOAT),'Nan'), 0) AS sport_other_number,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Sport - Multiple - Number' AS FLOAT),'Nan'), 0) AS sport_multiple_number,
	
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Other - Area' AS FLOAT),'NaN'), 0) AS housing_other_area,
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Single Family Residence - Area' AS FLOAT),'NaN'), 0) AS housing_sfr_area,
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Collective Housing - Area' AS FLOAT),'NaN'), 0) AS housing_ch_area,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Healthcare - Multiple - Area' AS FLOAT),'NaN'), 0) AS care_multiple_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Healthcare - Other - Area' AS FLOAT),'NaN'), 0) AS care_other_area,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Healthcare - Public - Area' AS FLOAT),'NaN'), 0) AS care_public_area,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Educational - Superior - Area' AS FLOAT),'NaN'), 0) AS school_superior_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Educational - Other - Area' AS FLOAT),'NaN'), 0) AS school_other_area,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Educational - Basic - Area' AS FLOAT),'NaN'), 0) AS school_basic_area,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Area' AS FLOAT),'NaN'), 0) AS leisure_bar_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Cultural - Area' AS FLOAT),'NaN'), 0) AS leisure_cultural_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Other - Area' AS FLOAT),'NaN'), 0) AS leisure_other_area,
    COALESCE(NULLIF(cast(data ->> 'Destinations - Leisure - Shows - Area' AS FLOAT),'NaN'), 0) AS leisure_shows_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Mall - Area' AS FLOAT),'NaN'), 0) AS shopping_mall_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Other - Area' AS FLOAT),'NaN'), 0) AS shopping_other_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Market or Supermarket - Area' AS FLOAT),'NaN'), 0) AS shopping_market_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Retail - Stand Alone Retail - Area' AS FLOAT),'Nan'), 0) AS shopping_alone_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Sport - Other - Area' AS FLOAT),'Nan'), 0) AS sport_other_area,
	COALESCE(NULLIF(cast(data ->> 'Destinations - Sport - Multiple - Area' AS FLOAT),'Nan'), 0) AS sport_multiple_area,
					

	COALESCE(NULLIF(cast(data ->> 'Other - Office - Area' as FLOAT),'Nan'), 0) as office_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Industrial - Area' as FLOAT),'Nan'), 0) as industrial_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Storage - Area' as FLOAT),'Nan'), 0) as storage_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Parking - Area' as FLOAT),'Nan'), 0) as parking_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Unbuilt - Area' as FLOAT),'Nan'), 0) as unbuilt_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Hotel - Area' as FLOAT),'Nan'), 0) as hotel_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Religious - Area' as FLOAT),'Nan'), 0) as religious_area,
	COALESCE(NULLIF(cast(data ->> 'Other - Infrastructure - Other - Area' as FLOAT),'Nan'), 0) as infra_area,

	
	COALESCE(NULLIF(cast(data ->> 'Parcel Area' as float),'Nan'), 0) as parcel_area,
	COALESCE(NULLIF(cast(data ->> 'Parcel Built Area' as float),'Nan'), 0) as parcel_built_area,
	COALESCE(NULLIF(cast(data ->> 'Parcel Built Area Above Ground' as float),'Nan'), 0) as parcel_built_ag,
	COALESCE(NULLIF(cast(data ->> 'Building Year Built - mean' as float),'Nan'), 0) as year_built,

	geometry
	  FROM sources.pois
	  WHERE category = 'land use - general'
	)
,
loaded_pois AS (
  SELECT
    sd.census_id,
    pois.id AS poi_id,
	CEIL((pois.housing_other_number + pois.housing_sfr_number + pois.housing_ch_number) * sd.mean_household_size) population,
	(pois.housing_other_number + pois.housing_sfr_number + pois.housing_ch_number) as housing_number,
	pois.care_multiple_area + pois.care_other_area + pois.care_public_area as care_area,
	pois.school_superior_area + pois.school_other_area + pois.school_basic_area as school_area,
	pois.leisure_bar_area + pois.leisure_cultural_area + pois.leisure_other_area + pois.leisure_shows_area as leisure_area,
	pois.shopping_mall_area + pois.shopping_other_area + pois.shopping_market_area + pois.shopping_alone_area as shopping_area,
	pois.sport_other_area + pois.sport_multiple_area as sport_area,
	pois.geometry
	FROM pois 
	JOIN sociodemo_geo sd 
	ON ST_Contains(sd.geometry, pois.geometry)
	),

pois_table as (
select l.*,n.node_id, e.ego_900 from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id left join ego_graphs e on n.node_id = e.node_id
),
	
node_populations AS (
    SELECT cast(node_id as integer) node_id,
    SUM(population) as population,
	SUM(housing_number) as housing_number,
	SUM(care_area) as care_area,
	SUM(school_area) as school_area,
	SUM(leisure_area) as leisure_area,
	SUM(shopping_area) as shopping_area,
	SUM(sport_area) as sport_area
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
SUM(housing_number) as housing_number,
SUM(care_area) * 0.66 as care_trips,
SUM(school_area) * 0.03 as school_trips,
SUM(leisure_area) * 0.042 as leisure_trips,
SUM(shopping_area) * 0.024 as shopping_trips,
SUM(sport_area) * 0.04 as sport_trips
FROM unnested_ego
JOIN node_populations ON unnested_ego.node_id = node_populations.node_id 
GROUP BY unnested_ego.poi_id),

reloaded_pois as (select m.*,l.population local_population, l.geometry from metrics_on_ego m join loaded_pois l on m.poi_id = l.poi_id ),

final_pois as (
SELECT
g.id tz_id,
local_population,
housing_number,
ROUND(CAST(
case 
	when housing_number < care_trips then housing_number
	when care_trips < housing_number then care_trips
	when housing_number = care_trips then housing_number
end as numeric),2) as DIV_WT_CARE,
ROUND(CAST(
case 
	when housing_number < care_trips then care_trips - housing_number
	when housing_number > care_trips then (housing_number - care_trips)*(-1)
end as numeric),2) as DIV_UT_CARE,
ROUND(CAST(
case 
	when housing_number < school_trips then housing_number
	when school_trips < housing_number then school_trips
	when housing_number = school_trips then housing_number
end as numeric),2) as DIV_WT_SCHOOL,
ROUND(CAST(
case 
	when housing_number < school_trips then school_trips - housing_number
	when housing_number > school_trips then (housing_number - school_trips)*(-1)
end as numeric),2) as DIV_UT_SCHOOL,
ROUND(CAST(
case 
	when housing_number < leisure_trips then housing_number
	when leisure_trips < housing_number then leisure_trips
	when housing_number = leisure_trips then housing_number
end as numeric),2) as DIV_WT_LEISURE,
ROUND(CAST(
case 
	when housing_number < leisure_trips then leisure_trips - housing_number
	when housing_number > leisure_trips then (housing_number - leisure_trips)*(-1)
end as numeric),2) as DIV_UT_LEISURE,
ROUND(CAST(
case 
	when housing_number < shopping_trips then housing_number
	when shopping_trips < housing_number then shopping_trips
	when housing_number = shopping_trips then housing_number
end as numeric),2) as DIV_WT_SHOPPING,
ROUND(CAST(
case 
	when housing_number < shopping_trips then shopping_trips - housing_number
	when housing_number > shopping_trips then (housing_number - shopping_trips)*(-1)
end as numeric),2) as DIV_UT_SHOPPING,
ROUND(CAST(
case 
	when housing_number < sport_trips then housing_number
	when sport_trips < housing_number then sport_trips
	when housing_number = sport_trips then housing_number
end as numeric),2) as DIV_WT_SPORT,
ROUND(CAST(
case 
	when housing_number < sport_trips then sport_trips - housing_number
	when housing_number > sport_trips then (housing_number - sport_trips)*(-1)
end as numeric),2) as DIV_UT_SPORT

FROM reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

zones_aggregated as (
select
tz_id,
sum(housing_number) as housing_number,
ROUND(cast(SUM(DIV_WT_CARE * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_WT_CARE,
ROUND(cast(SUM(DIV_UT_CARE * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_UT_CARE,
ROUND(cast(SUM(DIV_WT_SCHOOL * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_WT_SCHOOL,
ROUND(cast(SUM(DIV_UT_SCHOOL * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_UT_SCHOOL,
ROUND(cast(SUM(DIV_WT_LEISURE * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_WT_LEISURE,
ROUND(cast(SUM(DIV_UT_LEISURE * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_UT_LEISURE,
ROUND(cast(SUM(DIV_WT_SHOPPING * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_WT_SHOPPING,
ROUND(cast(SUM(DIV_UT_SHOPPING * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_UT_SHOPPING,
ROUND(cast(SUM(DIV_WT_SPORT * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_WT_SPORT,
ROUND(cast(SUM(DIV_UT_SPORT * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DIV_UT_SPORT

from final_pois
group by tz_id)

select z.*,zt.geometry
from zones_aggregated z join transportation_geo zt on z.tz_id = zt.id
--where dens_pop_total is not null




