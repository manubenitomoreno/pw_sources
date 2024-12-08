--DROP MATERIALIZED VIEW precomputed_pois_1500
CREATE MATERIALIZED VIEW precomputed_pois_1500 AS

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


WITH

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '1500'),',') as ego
from networks.amm_network_ego where relation_kind = 'ego_graphs'),

original_segments as (
SELECT
id original_id,
nullif(cast(data ->> 'length' as float),0) as segment_length
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
 SELECT node_id, cast(UNNEST(ego) as integer) as reachable_node_id
    FROM ego_graphs
),

distinct_reachable_edges AS (
    SELECT DISTINCT node_id, edge_id, original_id, segment_length, culdesac, slope, length
    FROM (

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
poi_id,
fm.degree as DES_DEGREE,
fm.total_reachable_length as DES_TOTAL_LENGTH,
fm.avg_segment_length as DES_BLOCK_LENGTH,
fm.pct_culdesac_length as DES_CULDESAC,
fm.avg_weighted_slope as DES_SLOPE,
pp.geometry
from precomputed_pois_1500 pp join final_metrics fm on pp.node_id = fm.node_id)
/*
final_pois as (
select 
tz_id,
ROUND(cast(SUM(fm.degree * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_DEGREE,
ROUND(cast(SUM(fm.total_reachable_length * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_TOTAL_LENGTH,
ROUND(cast(SUM(fm.avg_segment_length * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_BLOCK_LENGTH,
ROUND(cast(SUM(fm.pct_culdesac_length * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_CULDESAC,
ROUND(cast(SUM(fm.avg_weighted_slope * population) / nullif(sum(population),0)  as NUMERIC),0) as DES_SLOPE
from precomputed_pois_300 pp join final_metrics fm on pp.node_id = fm.node_id
group by tz_id)
*/
select * from final_pois
--select fp.*,tg.geometry from final_pois fp join transportation_geo tg on fp.tz_id = tg.id

