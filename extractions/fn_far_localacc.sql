--DELETE FROM sources.pois
WITH

nearest_poi as (

select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '1500'),',') as ego
--string_to_array(trim(both '[]' from data ->> '{proximity}'),',') as ego_{proximity}
from networks.amm_network_ego where relation_kind = 'ego_graphs'),

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
	pois.housing_other_number,
	pois.housing_sfr_number,
	pois.housing_ch_number,
	(pois.housing_other_number + pois.housing_sfr_number + pois.housing_ch_number) as housing_number,
	pois.care_multiple_number,
	pois.care_other_number,
	pois.care_public_number,
	pois.school_superior_number,
	pois.school_other_number,
	pois.school_basic_number,
	pois.leisure_bar_number,
	pois.leisure_cultural_number,
	pois.leisure_other_number,
	pois.leisure_shows_number,
	pois.shopping_mall_number,
	pois.shopping_other_number,
	pois.shopping_market_number,
	pois.shopping_alone_number,
	pois.sport_other_number,
	pois.sport_multiple_number,
	pois.housing_sfr_area,
	pois.housing_ch_area,
	pois.housing_other_area + pois.housing_sfr_area + pois.housing_ch_area as housing_area,
	pois.care_multiple_area,
	pois.care_other_area,
	pois.care_public_area,
	pois.school_superior_area,
	pois.school_other_area,
	pois.school_basic_area,
	pois.leisure_bar_area,
	pois.leisure_cultural_area,
	pois.leisure_other_area,
	pois.leisure_shows_area,
	pois.shopping_mall_area,
	pois.shopping_other_area,
	pois.shopping_market_area,
	pois.shopping_alone_area,
	pois.sport_other_area,
	pois.sport_multiple_area,
	pois.office_area,
	pois.industrial_area,
	pois.storage_area,
	pois.parking_area,
	pois.unbuilt_area,
	pois.hotel_area,
	pois.religious_area,
	pois.infra_area,
	pois.parcel_built_area parcel_built_area,
	pois.parcel_area parcel_area,
	pois.parcel_built_ag parcel_built_ag,
	pois.geometry
	FROM pois 
	JOIN sociodemo_geo sd 
	ON ST_Contains(sd.geometry, pois.geometry)
	),

pois_table as (
select l.*,n.node_id, e.ego from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id left join ego_graphs e on n.node_id = e.node_id
--select l.*,n.node_id, e.ego_{proximity} from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id left join ego_graphs e on n.node_id = e.node_id
),
	
node_populations AS (
    SELECT cast(node_id as integer) node_id,
    SUM(population) as population,
	SUM(housing_number) as housing_number,
	SUM(housing_other_number) as housing_other_number,
	SUM(housing_sfr_number) as housing_sfr_number,
	SUM(housing_ch_number) as housing_ch_number,
	SUM(care_multiple_number) as care_multiple_number,
	SUM(care_other_number) as care_other_number,
	SUM(care_public_number) as care_public_number,
	SUM(school_superior_number) as school_superior_number,
	SUM(school_other_number) as school_other_number,
	SUM(school_basic_number) as school_basic_number,
	SUM(leisure_bar_number) as leisure_bar_number,
	SUM(leisure_cultural_number) as leisure_cultural_number,
	SUM(leisure_other_number) as leisure_other_number,
	SUM(leisure_shows_number) as leisure_shows_number,
	SUM(shopping_mall_number) as shopping_mall_number,
	SUM(shopping_other_number) as shopping_other_number,
	SUM(shopping_market_number) as shopping_market_number,
	SUM(shopping_alone_number) as shopping_alone_number,
	SUM(sport_multiple_number) as sport_multiple_number,
	SUM(sport_other_number) as sport_other_number,
	SUM(housing_area) as housing_area,
	SUM(housing_sfr_area) as housing_sfr_area,
	SUM(housing_ch_area) as housing_ch_area,
	SUM(care_multiple_area) as care_multiple_area,
	SUM(care_other_area) as care_other_area,
	SUM(care_public_area) as care_public_area,
	SUM(school_superior_area) as school_superior_area,
	SUM(school_other_area) as school_other_area,
	SUM(school_basic_area) as school_basic_area,
	SUM(leisure_bar_area) as leisure_bar_area,
	SUM(leisure_cultural_area) as leisure_cultural_area,
	SUM(leisure_other_area) as leisure_other_area,
	SUM(leisure_shows_area) as leisure_shows_area,
	SUM(shopping_mall_area) as shopping_mall_area,
	SUM(shopping_other_area) as shopping_other_area,
	SUM(shopping_market_area) as shopping_market_area,
	SUM(shopping_alone_area) as shopping_alone_area,
	SUM(sport_other_area) as sport_other_area,
	SUM(sport_multiple_area) as sport_multiple_area,
	SUM(office_area) as office_area,
	SUM(industrial_area) as industrial_area,
	SUM(storage_area) as storage_area,
	SUM(parking_area) as parking_area,
	SUM(unbuilt_area) as unbuilt_area,
	SUM(hotel_area) as hotel_area,
	SUM(religious_area) as religious_area,
	SUM(infra_area) as infra_area,
    SUM(parcel_built_area) as parcel_built_area,
	SUM(parcel_area) as parcel_area,
	SUM(parcel_built_ag) as parcel_built_ag
    FROM pois_table
    GROUP BY node_id
),

unnested_ego AS (
    SELECT poi_id, cast(UNNEST(ego) as integer) as node_id
	--SELECT poi_id, cast(UNNEST(ego_{proximity}) as integer) as node_id
    FROM pois_table
),

metrics_on_ego as (
SELECT unnested_ego.poi_id,
SUM(population) population,
SUM(housing_number) as housing_number,
SUM(housing_other_number) as housing_other_number,
SUM(housing_sfr_number) as housing_sfr_number,
SUM(housing_ch_number) as housing_ch_number,
SUM(care_multiple_number) as care_multiple_number,
SUM(care_other_number) as care_other_number,
SUM(care_public_number) as care_public_number,
SUM(school_superior_number) as school_superior_number,
SUM(school_other_number) as school_other_number,
SUM(school_basic_number) as school_basic_number,
SUM(leisure_bar_number) as leisure_bar_number,
SUM(leisure_cultural_number) as leisure_cultural_number,
SUM(leisure_other_number) as leisure_other_number,
SUM(leisure_shows_number) as leisure_shows_number,
SUM(shopping_mall_number) as shopping_mall_number,
SUM(shopping_other_number) as shopping_other_number,
SUM(shopping_market_number) as shopping_market_number,
SUM(shopping_alone_number) as shopping_alone_number,
SUM(sport_other_number) as sport_multiple_number,
SUM(housing_area) as housing_area,
SUM(housing_sfr_area) as housing_sfr_area,
SUM(housing_ch_area) as housing_ch_area,
SUM(care_multiple_area) as care_multiple_area,
SUM(care_other_area) as care_other_area,
SUM(care_public_area) as care_public_area,
SUM(school_superior_area) as school_superior_area,
SUM(school_other_area) as school_other_area,
SUM(school_basic_area) as school_basic_area,
SUM(leisure_bar_area) as leisure_bar_area,
SUM(leisure_cultural_area) as leisure_cultural_area,
SUM(leisure_other_area) as leisure_other_area,
SUM(leisure_shows_area) as leisure_shows_area,
SUM(shopping_mall_area) as shopping_mall_area,
SUM(shopping_other_area) as shopping_other_area,
SUM(shopping_market_area) as shopping_market_area,
SUM(shopping_alone_area) as shopping_alone_area,
SUM(sport_other_area) as sport_other_area,
SUM(sport_multiple_area) as sport_multiple_area,
SUM(office_area) as office_area,
SUM(industrial_area) as industrial_area,
SUM(storage_area) as storage_area,
SUM(parking_area) as parking_area,
SUM(unbuilt_area) as unbuilt_area,
SUM(hotel_area) as hotel_area,
SUM(religious_area) as religious_area,
SUM(infra_area) as infra_area,
SUM(parcel_built_area) as parcel_built_area,
SUM(parcel_area) as parcel_area,
SUM(parcel_built_ag) as parcel_built_ag
FROM unnested_ego
JOIN node_populations ON unnested_ego.node_id = node_populations.node_id 
GROUP BY unnested_ego.poi_id),

reloaded_pois as (
select m.*,l.population local_population, l.geometry from metrics_on_ego m join loaded_pois l on m.poi_id = l.poi_id),

final_pois as (
SELECT
g.id tz_id,
p.poi_id,
local_population,
p.population DENS_POP_TOTAL,
p.housing_number DENS_HOU_TOTAL,
ROUND(CAST(p.parcel_built_area / nullif(p.parcel_area,0) AS NUMERIC),2) DENS_FAR,
ROUND(CAST(p.parcel_built_ag / nullif(p.parcel_area,0) AS NUMERIC), 2) DENS_FAR_AG,
ROUND(CAST((p.unbuilt_area*100) / nullif(p.parcel_area,0) AS NUMERIC),2) DEN_PERC_UNBUILT,
p.parcel_built_area DENS_BUILT_TOTAL,
ROUND(CAST((p.housing_sfr_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_HOUSING_SFR,
ROUND(CAST((p.housing_ch_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_HOUSING_CH,
ROUND(CAST((p.care_multiple_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_CARE_OTHER,
ROUND(CAST((p.care_public_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_CARE_PUBLIC,
ROUND(CAST((p.school_superior_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_SCHOOL_SUPERIOR,
ROUND(CAST((p.school_basic_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_SCHOOL_BASIC,
ROUND(CAST((p.leisure_bar_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_LEISURE_BAR,
ROUND(CAST((p.leisure_cultural_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_LEISURE_CULTURAL,
ROUND(CAST((p.leisure_shows_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_LEISURE_SHOWS,
ROUND(CAST((p.shopping_mall_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_SHOPPING_MALL,
ROUND(CAST((p.shopping_market_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_SHOPPING_MARKET,
ROUND(CAST((p.shopping_alone_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_SHOPPING_ALONE,
ROUND(CAST((p.sport_multiple_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_SPORT_OTHER,
ROUND(CAST((p.office_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_OFFICE,
ROUND(CAST((p.industrial_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_INDUSTRIAL,
ROUND(CAST((p.storage_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_STORAGE,
ROUND(CAST((p.parking_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_PARKING,
ROUND(CAST((p.hotel_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_HOTEL,
ROUND(CAST((p.religious_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_RELIGIOUS,
ROUND(CAST((p.infra_area*100) / nullif(p.parcel_built_area,0) AS NUMERIC),2) DEN_PERC_INFRA,

care_multiple_number ACC_CARE_OTHER,
care_public_number ACC_CARE_PUBLIC,
school_superior_number ACC_SCHOOL_SUPERIOR,
school_basic_number ACC_SCHOOL_BASIC,
leisure_bar_number ACC_LEISURE_BAR,
leisure_cultural_number ACC_LEISURE_CULTURAL,
leisure_shows_number ACC_LEISURE_SHOWS,
shopping_mall_number ACC_SHOPPING_MALL,
shopping_market_number ACC_SHOPPING_MARKET,
shopping_alone_number ACC_SHOPPING_ALONE,
sport_multiple_number ACC_SPORT_OTHER,
p.geometry
FROM reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
)

SELECT * FROM final_pois

/*
zones_aggregated as (
select
tz_id,
ROUND(cast(SUM(DENS_POP_TOTAL * local_population) / nullif(sum(local_population),0)  as NUMERIC),0) as DENS_POP_TOTAL,
ROUND(cast(SUM(DENS_HOU_TOTAL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DENS_HOU_TOTAL,
ROUND(cast(SUM(DENS_FAR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DENS_FAR,
ROUND(cast(SUM(DENS_FAR_AG * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DENS_FAR_AG,
ROUND(cast(SUM(DEN_PERC_UNBUILT * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_UNBUILT,
ROUND(cast(SUM(DENS_BUILT_TOTAL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DENS_BUILT_TOTAL,
ROUND(cast(SUM(DEN_PERC_HOUSING_SFR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_HOUSING_SFR,
ROUND(cast(SUM(DEN_PERC_HOUSING_CH * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_HOUSING_CH,
ROUND(cast(SUM(DEN_PERC_CARE_OTHER * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_CARE_OTHER,
ROUND(cast(SUM(DEN_PERC_CARE_PUBLIC * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_CARE_PUBLIC,
ROUND(cast(SUM(PERC_FAR_SCHOOL_SUPERIOR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_FAR_SCHOOL_SUPERIOR,
ROUND(cast(SUM(DEN_PERC_SCHOOL_BASIC * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_SCHOOL_BASIC,
ROUND(cast(SUM(DEN_PERC_LEISURE_BAR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_LEISURE_BAR,
ROUND(cast(SUM(DEN_PERC_LEISURE_CULTURAL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_LEISURE_CULTURAL,
ROUND(cast(SUM(DEN_PERC_LEISURE_SHOWS * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_LEISURE_SHOWS,
ROUND(cast(SUM(DEN_PERC_SHOPPING_MALL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_SHOPPING_MALL,
ROUND(cast(SUM(DEN_PERC_SHOPPING_MARKET * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_SHOPPING_MARKET,
ROUND(cast(SUM(DEN_PERC_SHOPPING_ALONE * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_SHOPPING_ALONE,
ROUND(cast(SUM(DEN_PERC_SPORT_OTHER * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_SPORT_OTHER,
ROUND(cast(SUM(DEN_PERC_OFFICE * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_OFFICE,
ROUND(cast(SUM(DEN_PERC_INDUSTRIAL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_INDUSTRIAL,
ROUND(cast(SUM(DEN_PERC_STORAGE * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_STORAGE,
ROUND(cast(SUM(DEN_PERC_PARKING * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_PARKING,
ROUND(cast(SUM(DEN_PERC_HOTEL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_HOTEL,
ROUND(cast(SUM(DEN_PERC_INFRA * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_INFRA,
ROUND(cast(SUM(DEN_PERC_RELIGIOUS * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as DEN_PERC_RELIGIOUS,
ROUND(cast(SUM(ACC_CARE_OTHER * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_CARE_OTHER,
ROUND(cast(SUM(ACC_CARE_PUBLIC * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_CARE_PUBLIC,
ROUND(cast(SUM(ACC_SCHOOL_SUPERIOR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_SCHOOL_SUPERIOR,
ROUND(cast(SUM(ACC_SCHOOL_BASIC * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_SCHOOL_BASIC,
ROUND(cast(SUM(ACC_LEISURE_BAR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_LEISURE_BAR,
ROUND(cast(SUM(ACC_LEISURE_CULTURAL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_LEISURE_CULTURAL,
ROUND(cast(SUM(ACC_LEISURE_SHOWS * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_LEISURE_SHOWS,
ROUND(cast(SUM(ACC_SHOPPING_MALL * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_SHOPPING_MALL,
ROUND(cast(SUM(ACC_SHOPPING_MARKET * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_SHOPPING_MARKET,
ROUND(cast(SUM(ACC_SHOPPING_ALONE * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_SHOPPING_ALONE,
ROUND(cast(SUM(ACC_SPORT_OTHER * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as ACC_SPORT_OTHER
from final_pois
group by tz_id)

select z.*,zt.geometry
from zones_aggregated z join transportation_geo zt on z.tz_id = zt.id
where dens_pop_total is not null
*/



