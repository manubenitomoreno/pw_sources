--DELETE FROM sources.pois

WITH
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
    tg.id tz_id,
    pois.id AS poi_id,
    pois.housing_other_number,
pois.housing_sfr_number,
pois.housing_ch_number,
pois.housing_other_number + pois.housing_sfr_number + pois.housing_ch_number as housing_number,
pois.care_multiple_number,
pois.care_other_number,
pois.care_public_number,
pois.care_multiple_number + pois.care_other_number + pois.care_public_number as care_number,
pois.school_superior_number,
pois.school_other_number,
pois.school_basic_number,
pois.school_superior_number + pois.school_other_number + pois.school_basic_number as school_number,
pois.leisure_bar_number,
pois.leisure_cultural_number,
pois.leisure_other_number,
pois.leisure_shows_number,
pois.leisure_bar_number + pois.leisure_cultural_number + pois.leisure_other_number + pois.leisure_shows_number as leisure_number,
pois.shopping_mall_number,
pois.shopping_other_number,
pois.shopping_market_number,
pois.shopping_alone_number,
pois.shopping_mall_number + pois.shopping_other_number + pois.shopping_market_number + pois.shopping_alone_number as shopping_number,
pois.sport_other_number,
pois.sport_multiple_number,
pois.sport_other_number + pois.sport_multiple_number as sport_number,
	
pois.housing_other_area,
pois.housing_sfr_area,
pois.housing_ch_area,
pois.housing_other_area + pois.housing_sfr_area + pois.housing_ch_area as housing_area,
pois.care_multiple_area,
pois.care_other_area,
pois.care_public_area,
pois.care_multiple_area + pois.care_other_area + pois.care_public_area as care_area,
pois.school_superior_area,
pois.school_other_area,
pois.school_basic_area,
pois.school_superior_area + pois.school_other_area + pois.school_basic_area as school_area,
pois.leisure_bar_area,
pois.leisure_cultural_area,
pois.leisure_other_area,
pois.leisure_shows_area,
pois.leisure_bar_area + pois.leisure_cultural_area + pois.leisure_other_area + pois.leisure_shows_area as leisure_area,
pois.shopping_mall_area,
pois.shopping_other_area,
pois.shopping_market_area,
pois.shopping_alone_area,
pois.shopping_mall_area + pois.shopping_other_area + pois.shopping_market_area + pois.shopping_alone_area as shopping_area,
pois.sport_other_area,
pois.sport_multiple_area,
pois.sport_other_area + pois.sport_multiple_area as sport_area,
pois.office_area,
pois.industrial_area,
pois.storage_area,
pois.parking_area,
pois.unbuilt_area,
pois.hotel_area,
pois.religious_area,
pois.infra_area,
/*
pois.parcel_built_area - (
pois.housing_other_area + pois.housing_sfr_area + pois.housing_ch_area + pois.care_multiple_area + pois.care_other_area +
pois.care_public_area + pois.school_superior_area + pois.school_other_area + pois.school_basic_area + pois.leisure_bar_area + pois.leisure_cultural_area + pois.leisure_other_area +
pois.leisure_shows_area + pois.shopping_mall_area + pois.shopping_other_area + pois.shopping_market_area + pois.shopping_alone_area + pois.sport_other_area + pois.sport_multiple_area +
pois.office_area + pois.industrial_area + pois.storage_area + pois.parking_area + pois.unbuilt_area + pois.hotel_area + pois.religious_area + pois.infra_area )
as other_area,
*/
pois.parcel_built_area parcel_built_area,
pois.parcel_area parcel_area,
pois.parcel_built_ag parcel_built_ag,
pois.year_built

FROM pois 
JOIN transportation_geo tg 
ON ST_Contains(tg.geometry, pois.geometry)
),

--SELECT * FROM loaded_pois
loaded_tz AS(
SELECT 
  tz_id AS boundary_id,
    SUM(pois.housing_number) housing_number,
    SUM(pois.care_number) care_number,
    SUM(pois.school_number) school_number,
    SUM(pois.leisure_number) leisure_number,
	SUM(pois.shopping_number) shopping_number,
	SUM(pois.sport_number) sport_number,
	
	SUM(pois.housing_area) housing_area,
	SUM(pois.housing_area) housing_ch_area,
	SUM(pois.housing_area) housing_sfr_area,
	SUM(pois.housing_area) housing_other_area,
    SUM(pois.care_area) care_area,
	SUM(pois.care_multiple_area) care_multiple_area,
	SUM(pois.care_other_area) care_other_area,
	SUM(pois.care_public_area) care_public_area,
    SUM(pois.school_area) school_area,
	SUM(pois.school_superior_area) school_superior_area,
	SUM(pois.school_basic_area) school_basic_area,
	SUM(pois.school_other_area) school_other_area,
    SUM(pois.leisure_area) leisure_area,
	SUM(pois.leisure_bar_area) leisure_bar_area,
	SUM(pois.leisure_cultural_area) leisure_cultural_area,
	SUM(pois.leisure_other_area) leisure_other_area,
	SUM(pois.leisure_shows_area) leisure_shows_area,
	SUM(pois.shopping_area) shopping_area,
	SUM(pois.shopping_mall_area) shopping_mall_area,
	SUM(pois.shopping_other_area) shopping_other_area,
	SUM(pois.shopping_market_area) shopping_market_area,
	SUM(pois.shopping_alone_area) shopping_alone_area,
	SUM(pois.sport_area) sport_area,
	SUM(pois.office_area) office_area,
	SUM(pois.industrial_area) industrial_area,
	SUM(pois.storage_area) storage_area,
	SUM(pois.parking_area) parking_area,
	SUM(pois.unbuilt_area) unbuilt_area,
	SUM(pois.hotel_area) hotel_area,
	SUM(pois.religious_area) religious_area,
	SUM(pois.infra_area) infra_area,
	--SUM(pois.other_area) other_area,
	SUM(pois.parcel_area) parcel_area,
	(
	SUM(pois.housing_area)+ SUM(pois.care_area) + SUM(pois.school_area) + SUM(pois.leisure_area) +
	SUM(pois.shopping_area) + SUM(pois.office_area) + SUM(pois.industrial_area) + SUM(pois.storage_area) +
	SUM(pois.parking_area) + SUM(pois.unbuilt_area) + SUM(pois.hotel_area) + SUM(pois.religious_area) +
	SUM(pois.infra_area) --+ SUM(pois.other_area)
	) as parcel_effective_built_area,
	SUM(pois.parcel_built_area) parcel_built_area,
	SUM(pois.parcel_built_ag) parcel_built_ag,
	AVG(pois.year_built) year_built
	
FROM loaded_pois pois
GROUP BY tz_id)

SELECT * FROM loaded_tz where housing_number != 0

 /*
ALTER TABLE sources.boundaries_geo DROP COLUMN sid;
ALTER TABLE sources.pois DROP COLUMN sid;

CREATE INDEX sid
  ON sources.boundaries_geo 
  USING GIST (geometry);
 
CREATE INDEX sid
  ON sources.pois 
  USING GIST (geometry);

--delete from sources.pois where category = 'land use - general'


--delete from sources.boundaries_geo


DRoP TABLE sociodemo;
DROP TABLE geo;
DROP TABLE transportation_geo;
DROP TABLE sociodemo_geo;
DROP TABLE pois;
DROP TABLE loaded_pois;
DROP TABLE pois_all_geo;

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

CREATE TEMP TABLE
geo as (
select
split_part(id,'-',3) boundary_id,
geometry
from sources.boundaries_geo
where sources.boundaries_geo.category = 'boundary-census_tract'
and split_part(id,'-',3) in (select boundary_id from sociodemo));

CREATE TEMP TABLE
transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone');

CREATE TEMP TABLE
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
from sociodemo join geo on sociodemo.boundary_id = geo.boundary_id);

CREATE TEMP TABLE
pois as (
select id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT) +
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing_number,
cast(data ->> 'Destinations - Healthcare - Multiple - Number' as FLOAT) +
cast(data ->> 'Destinations - Healthcare - Public - Number' as FLOAT) as care_number,
cast(data ->> 'Destinations - Educational - Superior - Number' as FLOAT) +
cast(data ->> 'Destinations - Educational - Basic - Number' as FLOAT) as school_number,
cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Number' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Cultural - Number' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Shows - Number' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Shows - Number' as FLOAT) as leisure_number,
cast(data ->> 'Destinations - Retail - Mall - Number' as FLOAT) + 
cast(data ->> 'Destinations - Retail - Market or Supermarket - Number' as FLOAT) +
cast(data ->> 'Destinations - Retail - Stand Alone Retail - Number' as FLOAT) as shopping_number,
cast(data ->> 'Destinations - Sport - Multiple - Number' as FLOAT) as sport_number,
cast(data ->> 'Origins - Housing - Single Family Residence - Area' as FLOAT) +
cast(data ->> 'Origins - Housing - Collective Housing - Area' as FLOAT) as housing_area,	
cast(data ->> 'Destinations - Healthcare - Multiple - Area' as FLOAT) +
cast(data ->> 'Destinations - Healthcare - Public - Area' as FLOAT) as care_area,
cast(data ->> 'Destinations - Educational - Superior - Area' as FLOAT) +
cast(data ->> 'Destinations - Educational - Basic - Area' as FLOAT) as school_area,
cast(data ->> 'Destinations - Leisure - Bars and Restaurants - Area' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Cultural - Area' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Shows - Area' as FLOAT) +
cast(data ->> 'Destinations - Leisure - Shows - Area' as FLOAT) as leisure_area,
cast(data ->> 'Destinations - Retail - Mall - Area' as FLOAT) + 
cast(data ->> 'Destinations - Retail - Market or Supermarket - Area' as FLOAT) +
cast(data ->> 'Destinations - Retail - Stand Alone Retail - Area' as FLOAT) as shopping_area,
cast(data ->> 'Destinations - Sport - Multiple - Area' as FLOAT) as sport_area,
cast(data ->> 'Destinations - Public - Multiple - Area' as FLOAT) +
cast(data ->> 'Other - Office - Area' as FLOAT) +
cast(data ->> 'Other - Industrial - Area' as FLOAT) as work_area,
cast(data ->> 'Other - Storage - Area' as FLOAT) as storage_area,
cast(data ->> 'Other - Parking - Area' as FLOAT) as parking_area,
cast(data ->> 'Parcel Area' as float) as parcel_area,
cast(data ->> 'Parcel Built Area' as float) as parcel_built_area,
cast(data ->> 'Parcel Built Area Above Ground' as float) as parcel_built_ag,
cast(data ->> 'Building Year Built' as float) as year_built,
geometry
from sources.pois where category = 'land use - general' --limit 1000
);

CREATE TEMP TABLE
loaded_pois as (
select
sd.boundary_id,
pois.id poi_id,
coalesce(NULLIF(pois.housing_number,'NaN'),0) housing_number,
coalesce(CEIL(NULLIF(pois.housing_number,'NaN') * sd.mean_household_size),0) population,
coalesce(NULLIF(pois.care_number,'NaN'),0) care_number,
coalesce(NULLIF(pois.school_number,'NaN'),0) school_number,
coalesce(NULLIF(pois.leisure_number,'NaN'),0) leisure_number,
coalesce(NULLIF(pois.shopping_number,'NaN'),0) shopping_number,
coalesce(NULLIF(pois.sport_number,'NaN'),0) sport_number,
coalesce(NULLIF(pois.housing_area,'NaN'),0) housing_area,
coalesce(NULLIF(pois.care_area,'NaN'),0) care_area,
coalesce(NULLIF(pois.school_area,'NaN'),0) school_area,
coalesce(NULLIF(pois.leisure_area,'NaN'),0) leisure_area,
coalesce(NULLIF(pois.shopping_area,'NaN'),0) shopping_area,
coalesce(NULLIF(pois.sport_area,'NaN'),0) sport_area,
coalesce(NULLIF(pois.work_area,'NaN'),0) work_area,
coalesce(NULLIF(pois.storage_area,'NaN'),0) storage_area,
coalesce(NULLIF(pois.parking_area,'NaN'),0) parking_area,
pois.parcel_area parcel_area,
pois.parcel_built_area parcel_built_area,
pois.parcel_built_ag parcel_built_ag,
rent_hog_gross,
mean_household_size,
p_households_1,
mean_age,
coalesce(ceil(((NULLIF(pois.housing_number,'NaN') * sd.mean_household_size) * p_age_18)/100),0) population_age_18,
coalesce(ceil(((NULLIF(pois.housing_number,'NaN') * sd.mean_household_size) * p_age_65)/100),0) population_age_65,
pois.geometry
from pois, sociodemo_geo sd
where st_contains(sd.geometry,pois.geometry));
create temp table
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
);

--select * from pois_all_geo
--final as (	
select
tz_id boundary_id,
'taz' boundary_type,
sum(housing_number) housing_number,
sum(population) population,
sum(care_number) care_number,
sum(school_number) school_number,
sum(leisure_number) leisure_number,
sum(shopping_number) shopping_number,
sum(sport_number) sport_number,
sum(housing_area) housing_area,
sum(care_area) care_area,
sum(school_area) school_area,
sum(leisure_area) leisure_area,
sum(shopping_area) shopping_area,
sum(sport_area) sport_area,
sum(work_area) work_area,
sum(storage_area) storage_area,
sum(parking_area) parking_area,

sum(parcel_area) parcel_area,
sum(parcel_built_area) parcel_built_area,
sum(parcel_built_ag) parcel_built_ag

from pois_all_geo
group by tz_id



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
