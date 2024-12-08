--DROP MATERIALIZED VIEW precomputed_parks_300
CREATE MATERIALIZED VIEW precomputed_parks_1500 AS

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
-- Seleccionar los parques de acuerdo con el tipo
parks AS (
  SELECT 
  	id,
	CONCAT(SPLIT_PART(id, '-', 2), '-', SPLIT_PART(id, '-', 3), '-', SPLIT_PART(id, '-', 4), '-', SPLIT_PART(id, '-', 5)) park_id,
	split_part(id, '-', -1) park_type,
	geometry
  FROM sources.pois
  WHERE category = 'land use - park'
),

sociodemo as
(
  SELECT
    split_part(id,'-',1) boundary_id,
    nullif(greatest(cast(data ->> 'population' as float),0),0) as population,
    nullif(greatest(cast(data ->> 'mean_household_size' as float ),0),0) as mean_household_size
  FROM sources.boundaries_data bd 
  WHERE category = 'sociodemographic'
),

census_geo as (
  SELECT
    split_part(id,'-',3) boundary_id,
    geometry
  FROM sources.boundaries_geo
  WHERE category = 'boundary-census_tract'
  AND split_part(id,'-',3) IN (SELECT boundary_id FROM sociodemo)
),

sociodemo_geo as (
  SELECT
    sociodemo.boundary_id census_id,
    population,
    mean_household_size,
    census_geo.geometry
  FROM sociodemo 
  JOIN census_geo 
  ON sociodemo.boundary_id = census_geo.boundary_id
),

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
),

ego_graphs AS (
  SELECT 
    CAST(SPLIT_PART(relation_id, '|', 2) AS INTEGER) AS node_id,
    ARRAY(SELECT (jsonb_array_elements_text((data->>'1500')::jsonb))::INT) AS reachable_nodes
  FROM networks.amm_network_ego 
  WHERE relation_kind = 'ego_graphs'
),

nearest_parks AS (
  SELECT --DISTINCT
  	p.id,
    np.node_id,
    p.park_id,
    p.park_type,
	ST_Distance(p.geometry, np.geometry) distance
  FROM 
    (SELECT * FROM networks.amm_network_nodes) np
  JOIN 
    parks p 
  ON 
    ST_DWithin(np.geometry, p.geometry, 100) -- Ajusta la distancia según tu caso
),
-- Subquery to select the closest node to each park
final_nearest_parks AS (
  SELECT DISTINCT ON (np.id) 
    np.id,
	np.node_id,
    np.park_id,
    np.park_type
    --np.geometry
  FROM nearest_parks np
  ORDER BY np.id,np.distance
),

reachable_parks AS (
  SELECT 
    eg.node_id,  -- The original node from which reachable nodes are calculated
    reachable_node_id,  -- The unnested reachable node
    np.park_id,  -- Park ID from the nearest park
    np.park_type  -- Park type (Big or Small)
  FROM 
    ego_graphs eg
  -- Unnest the reachable_nodes array into rows using CROSS JOIN LATERAL
  CROSS JOIN LATERAL unnest(eg.reachable_nodes) AS reachable_node_id
  JOIN 
    final_nearest_parks np  -- Join with the parks
  ON 
    np.node_id = reachable_node_id ),

-- Contar los parques únicos en cada nodo y su tipo
unique_parks_per_node AS (
  SELECT 
    node_id,
    park_type,
    COUNT(DISTINCT park_id) AS unique_parks_count
  FROM 
    reachable_parks
  GROUP BY 
    node_id, park_type)


SELECT 
p.*, u.park_type, u.unique_parks_count 
FROM pois_table p left join unique_parks_per_node u on p.node_id = u.node_id ;
--------------------------------------------------

WITH 
transportation_geo AS (
  SELECT * 
  FROM sources.boundaries_geo 
  WHERE category = 'boundary-transportation_zone'
),

pois_tz as (
SELECT
pp.*,id tz_id
FROM precomputed_parks_1500 pp
JOIN transportation_geo sd 
ON sd.geometry && pp.geometry
AND ST_Within(pp.geometry, sd.geometry))--,

SELECT * FROM pois_tz


/*
zones_aggregated as (
select
tz_id,park_type,
ROUND(cast(SUM(unique_parks_count * population) / nullif(sum(population),0)  as NUMERIC),0) as ACC_PARK
from pois_tz
group by tz_id,park_type)

select z.*,st_Astext(tg.geometry) geometry from zones_aggregated z 
left join transportation_geo tg on z.tz_id = tg.id where park_type is not null

*/