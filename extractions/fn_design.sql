--DELETE FROM networks.amm_network_edges;
--DELETE FROM networks.amm_network_nodes;
--DELETE FROM networks.amm_network_ego;
--DELETE FROM networks.amm_network_length;
--DELETE FROM networks.amm_network_relations;



--SELECT * FROM networks.amm_network_edges

/*
CREATE MATERIALIZED VIEW precomputed_pois_all AS

WITH 

nearest_poi as (

select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),

pois AS (
  SELECT 
    id as poi_id, 
    COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Other - Number' AS FLOAT),'NaN'), 0) +
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Single Family Residence - Number' AS FLOAT),'NaN'), 0) +
	COALESCE(NULLIF(cast(data ->> 'Origins - Housing - Collective Housing - Number' AS FLOAT),'NaN'), 0) AS housing_number,
	geometry
	FROM sources.pois
	WHERE category = 'land use - general'
	),

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

loaded_pois AS (
  SELECT
    sd.census_id,
    pois.poi_id AS poi_id,
	CEIL(pois.housing_number * sd.mean_household_size) population,
	pois.geometry
	FROM pois 
	JOIN sociodemo_geo sd 
	ON ST_Contains(sd.geometry, pois.geometry)
	),

pois_table as (
select l.*,n.node_id from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id
)
,
transportation_geo AS (
  SELECT * 
  FROM sources.boundaries_geo 
  WHERE category = 'boundary-transportation_zone'
),

pois_tz as (
SELECT
pois_table.*,id tz_id
FROM pois_table
JOIN transportation_geo sd 
ON sd.geometry && pois_table.geometry
AND ST_Within(pois_table.geometry, sd.geometry))--,

select * from pois_tz;
*/
----------------------------------------------
WITH

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '300'),',') as ego_300
from networks.amm_network_ego where relation_kind = 'ego_graphs'),

original_segments as (
SELECT
id original_id,
nullif(cast(data ->> 'length' as float),-999) as segment_length
FROM sources.road_segments
),

edge_metrics AS (
select
edge_id,
e.start node_start,
e.end node_end,
COALESCE(NULLIF(cast(data ->> 'length' AS NUMERIC),'NaN'), 0) AS length,
COALESCE(NULLIF(cast(data ->> 'original_id' AS NUMERIC),'NaN'), 0) AS original_id,
COALESCE(NULLIF(cast(data ->> 'slope' AS FLOAT),'NaN'), 0) AS slope,
COALESCE(NULLIF(cast(data ->> 'culdesac' AS FLOAT),'NaN'), 0) AS culdesac,
segment_length
from networks.amm_network_edges e
left join original_segments os on
COALESCE(NULLIF(cast(data ->> 'original_id' AS NUMERIC),'NaN'), 0) = os.original_id
),

node_metrics as (
select 
node_id,
e.*
from networks.amm_network_nodes
left join edge_metrics e
on node_id = node_start

union all

select 
node_id,
e.*
from networks.amm_network_nodes
left join edge_metrics e
on node_id = node_end

),

unnested_ego AS (
 SELECT node_id, cast(UNNEST(ego_300) as integer) as reachable_node_id
    FROM ego_graphs
),

distinct_reachable_edges AS (
    SELECT DISTINCT node_id, edge_id, original_id, segment_length, culdesac, slope, length
    FROM (
        -- Your existing final table query here
        -- Replace the following line with your query that generates the final table:
        SELECT u.node_id, nm.edge_id, nm.original_id, nm.segment_length, nm.culdesac, nm.slope, nm.length
        FROM unnested_ego u
        LEFT JOIN node_metrics nm ON u.node_id = nm.node_id
    ) AS final_table
),

calculated_metrics AS (
    SELECT 
        node_id,
        SUM(length) AS total_reachable_length,
        AVG(segment_length) AS avg_segment_length,
        100.0 * SUM(CASE WHEN culdesac = 1 THEN length ELSE 0 END) / NULLIF(SUM(length), 0) AS pct_culdesac_length,
        SUM(slope * length) / NULLIF(SUM(length), 0) AS avg_weighted_slope
    FROM distinct_reachable_edges
    GROUP BY node_id
),
final_metrics as (
SELECT 
    an.node_id,
	COALESCE(NULLIF(cast(an.data ->> 'degree' AS NUMERIC),'NaN'), 0) AS degree,
	st_astext(an.geometry) geometry,
    total_reachable_length,
    avg_segment_length,
    pct_culdesac_length,
    avg_weighted_slope
FROM calculated_metrics cm join networks.amm_network_nodes an
on cm.node_id = an.node_id),

transportation_geo AS (
  SELECT * 
  FROM sources.boundaries_geo 
  WHERE category = 'boundary-transportation_zone'
),

final_pois as (
select 
tz_id,
ROUND(cast(SUM(fm.degree * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_DEGREE,
ROUND(cast(SUM(fm.total_reachable_length * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_TOTAL_LENGTH,
ROUND(cast(SUM(fm.avg_segment_length * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_BLOCK_LENGTH,
ROUND(cast(SUM(fm.pct_culdesac_length * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_CULDESAC,
ROUND(cast(SUM(fm.avg_weighted_slope * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_SLOPE
from precomputed_pois_all pp join final_metrics fm on pp.node_id = fm.node_id
group by tz_id)

select fp.*,tg.geometry from final_pois fp join transportation_geo tg on fp.tz_id = tg.id


/*
,

segment_length as (
SELECT 
    node_id,
    AVG(segment_length) AS avg_segment_length
FROM distinct_reachable_segments
GROUP BY node_id),

distinct_reachable_edges AS (
    SELECT DISTINCT node_id, edge_id, segment_length, culdesac, slope
    FROM (
        -- Your existing final table query here
        -- Replace the following line with your query that generates the final table:
        SELECT u.node_id, nm.edge_id, nm.segment_length, nm.culdesac, nm.slope
        FROM unnested_ego u
        LEFT JOIN node_metrics nm ON u.node_id = nm.node_id
    ) AS final_table
),


culdesac_metrics AS (
    SELECT 
        node_id,
        SUM(segment_length) AS total_reachable_length,
        SUM(CASE WHEN culdesac = 1 THEN segment_length ELSE 0 END) AS culdesac_length
    FROM distinct_reachable_edges
    GROUP BY node_id
),

weighted_slope AS (
    SELECT 
        edge_id,
        SUM(slope * segment_length) / NULLIF(SUM(segment_length), 0) AS avg_weighted_slope
    FROM distinct_reachable_edges
    GROUP BY edge_id
)

SELECT 
    cm.node_id,
	total_reachable_length,
	avg_weighted_slope,
    100.0 * culdesac_length / NULLIF(total_reachable_length, 0) AS pct_culdesac_length
FROM culdesac_metrics cm join weighted_slope ws on cm.node_id = ws.node_id;





distinct_reachable_nodes AS (
    SELECT DISTINCT node_id, reachable_node_id, node_degree
    FROM (
        -- Your existing final table query here
        -- Replace the following line with your query that generates the final table:
        SELECT u.node_id, u.reachable_node_id, nm.node_degree
        FROM unnested_ego u
        LEFT JOIN node_metrics nm ON u.node_id = nm.node_id
    ) AS final_table
)

SELECT 
    node_id,
    AVG(node_degree) AS avg_reachable_degree,
    100.0 * SUM(CASE WHEN node_degree = 4 THEN 1 ELSE 0 END) / COUNT(*) AS pct_degree_4_reachable
FROM distinct_reachable_nodes
GROUP BY node_id;

nearest_poi as (
select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),
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
	SUM(housing_number) as housing_number
    FROM pois_table
    GROUP BY node_id
),


metrics_on_ego as (
SELECT unnested_ego.poi_id,
--SUM(population) population,
--SUM(housing_number) as housing_number,
ROUND(cast(SUM(slope * segment_length) / nullif(sum(segment_length),0)  as NUMERIC),0) as slope,
ROUND(cast(SUM(culdesac * segment_length) / nullif(sum(segment_length),0)  as NUMERIC),0) as culdesac,
avg(segment_length), 
FROM unnested_ego
JOIN edge_metrics ON unnested_ego.node_id = edge_metrics.start 
GROUP BY unnested_ego.poi_id)

SELECT * FROM metrics_on_ego




---------------------------------
unnested_ego AS (
    SELECT poi_id, cast(UNNEST(ego_900) as integer) as node_id
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

reloaded_pois as (select m.*,l.population local_population, l.geometry from metrics_on_ego m join loaded_pois l on m.poi_id = l.poi_id ),

final_pois as (
SELECT
g.id tz_id,
local_population,
p.population DENS_POP_TOTAL,
p.housing_number DENS_HOU_TOTAL,
ROUND(CAST(p.parcel_built_area / p.parcel_area AS NUMERIC),2) DENS_FAR,
ROUND(CAST(p.parcel_built_ag / p.parcel_area AS NUMERIC), 2) DENS_FAR_AG,
ROUND(CAST((p.unbuilt_area*100) / p.parcel_area AS NUMERIC),2) DEN_PERC_UNBUILT,
p.parcel_built_area DENS_BUILT_TOTAL,
ROUND(CAST((p.housing_sfr_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_HOUSING_SFR,
ROUND(CAST((p.housing_ch_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_HOUSING_CH,
ROUND(CAST((p.care_multiple_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_CARE_OTHER,
ROUND(CAST((p.care_public_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_CARE_PUBLIC,
ROUND(CAST((p.school_superior_area*100) / p.parcel_built_area AS NUMERIC),2) PERC_FAR_SCHOOL_SUPERIOR,
ROUND(CAST((p.school_basic_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_SCHOOL_BASIC,
ROUND(CAST((p.leisure_bar_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_LEISURE_BAR,
ROUND(CAST((p.leisure_cultural_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_LEISURE_CULTURAL,
ROUND(CAST((p.leisure_shows_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_LEISURE_SHOWS,
ROUND(CAST((p.shopping_mall_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_SHOPPING_MALL,
ROUND(CAST((p.shopping_market_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_SHOPPING_MARKET,
ROUND(CAST((p.shopping_alone_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_SHOPPING_ALONE,
ROUND(CAST((p.sport_multiple_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_SPORT_OTHER,
ROUND(CAST((p.office_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_OFFICE,
ROUND(CAST((p.industrial_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_INDUSTRIAL,
ROUND(CAST((p.storage_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_STORAGE,
ROUND(CAST((p.parking_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_PARKING,
ROUND(CAST((p.hotel_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_HOTEL,
ROUND(CAST((p.religious_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_RELIGIOUS,
ROUND(CAST((p.infra_area*100) / p.parcel_built_area AS NUMERIC),2) DEN_PERC_INFRA,

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
sport_multiple_number ACC_SPORT_OTHER

FROM reloaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

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
ROUND(cast(SUM(PERC_FAR_SCHOOL_SUPERIOR * local_population) / nullif(sum(local_population),0) as NUMERIC),2) as PERC_FAR_SCHOOL_SUPERIOR,
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

