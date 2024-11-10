
--DROP MATERIALIZED VIEW precomputed_pois_300;
CREATE MATERIALIZED VIEW precomputed_pois_300 AS

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
nullif(GREATEST(cast(data ->> 'population' as float),0),0) as population,
nullif(GREATEST(cast(data ->> 'mean_household_size' as float ),0),0) as mean_household_size
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
	CEIL(pois.housing_number * sd.mean_household_size) population
	FROM pois 
	JOIN sociodemo_geo sd 
	ON ST_Contains(sd.geometry, pois.geometry)
	),

pois_table as (
select l.*,n.node_id from loaded_pois l left join nearest_poi n on l.poi_id = n.poi_id
),

t_pois AS (
    SELECT 
        DISTINCT ON (ts.id) 
        ts.id AS poi_id, 
        nn.node_id, 
        ARRAY(SELECT jsonb_array_elements_text((ts.data->'mode_lines')::jsonb)) AS mode_lines
    FROM 
        sources.pois ts
    JOIN 
        networks.amm_network_nodes nn 
    ON 
        ST_DWithin(ts.geometry, nn.geometry, 200)
    WHERE 
        ts.category = 'transportation'
    ORDER BY 
        ts.id, ST_Distance(ts.geometry, nn.geometry) ASC
),
lines_per_node AS (
    SELECT 
        node_id,
        ARRAY_AGG(DISTINCT ml.mode_line) AS combined_mode_lines
    FROM 
        t_pois,
        LATERAL unnest(mode_lines) AS ml(mode_line)
    GROUP BY 
        node_id
),

ego_graphs AS (
    SELECT 
        CAST(SPLIT_PART(relation_id, '|', 2) AS INTEGER) AS node_id,
        -- Convert '900' into an integer array (assuming it's a JSON array of node IDs)
        ARRAY(SELECT (jsonb_array_elements_text((data->>'300')::jsonb))::INT) AS reachable_nodes
    FROM 
        networks.amm_network_ego 
    WHERE 
        relation_kind = 'ego_graphs'
),
combined_modes AS (
    SELECT 
        eg.node_id,
        tp.combined_mode_lines AS node_mode_lines,
        reachable_node_id,
        tp_reachable.combined_mode_lines AS reachable_mode_lines
    FROM 
        ego_graphs eg
    -- Unnest the reachable_nodes array into rows
    CROSS JOIN LATERAL unnest(eg.reachable_nodes) AS reachable_node_id
    -- Join to get mode lines for the node itself
    LEFT JOIN lines_per_node tp 
    ON eg.node_id = tp.node_id 
    -- Join to get mode lines for reachable nodes
    LEFT JOIN lines_per_node tp_reachable 
    ON reachable_node_id = tp_reachable.node_id
),
-- Flatten and aggregate all the mode lines for each node
final_mode_lines AS (
    SELECT 
        node_id,
        ARRAY_AGG(DISTINCT mode_line) AS all_mode_lines
    FROM (
        SELECT 
            node_id,
            unnest(node_mode_lines) AS mode_line
        FROM combined_modes
        
        UNION ALL

        SELECT 
            node_id,
            unnest(reachable_mode_lines) AS mode_line
        FROM combined_modes
    ) AS flattened_modes
    GROUP BY node_id
)
,
transit_count as (
SELECT 
    fl.node_id,
    array_length(all_mode_lines, 1) AS acc_transportation,
	geometry
FROM 
    final_mode_lines fl 
join (select node_id, geometry from networks.amm_network_nodes) nn on fl.node_id = nn.node_id)


select poi_id,population,acc_transportation,geometry from pois_table np
join transit_count tc on np.node_id=tc.node_id
;
-----------------------------------
WITH 
transportation_geo AS (
  SELECT * 
  FROM sources.boundaries_geo 
  WHERE category = 'boundary-transportation_zone'
),

pois_tz as (
SELECT
pp.*,id tz_id
FROM precomputed_pois_300 pp
JOIN transportation_geo sd 
ON sd.geometry && pp.geometry
AND ST_Within(pp.geometry, sd.geometry))--,

SELECT * FROM pois_tz

/*
zones_aggregated as (
select
tz_id,
ROUND(cast(SUM(unique_mode_lines_count * population) / nullif(sum(population),0)  as NUMERIC),0) as ACC_TRANSIT
from pois_tz
group by tz_id)

select z.*,st_Astext(tg.geometry) geometry from zones_aggregated z left join transportation_geo tg on z.tz_id = tg.id 
*/
