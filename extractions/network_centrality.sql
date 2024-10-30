
with

transportation_geo as (select * from sources.boundaries_geo bg where category = '{zone_name}'),
pois as (
select id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as FLOAT)
+ 
cast(data ->> 'Origins - Housing - Collective Housing - Number' as FLOAT) as housing,
geometry
from sources.pois where category = 'land use - general'
),

nearest_poi as (
select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.amm_network_relations anr where relation_kind = 'nearest_poi'),

length_paths AS (
    SELECT
        split_part(relation_id, '|', 2)::integer AS start_node_id,
        kv.key::integer AS end_node_id,
        kv.value::float AS path_length
    FROM
        networks.amm_network_length anp,
        jsonb_each_text(anp.data) AS kv
    WHERE
        anp.relation_kind = 'length' and kv.value::float < 100
),
euclidean_paths as (
select l.*,
st_length(ST_MakeLine(ans.geometry,ane.geometry)) euclidean_length
from length_paths l
join networks.amm_network_nodes ans on l.start_node_id = ans.node_id ::integer
join networks.amm_network_nodes ane on l.end_node_id = ane.node_id ::integer),

straightness_centrality as (

select start_node_id node_id, (sum(euclidean_length)/sum(path_length)) straightness
from euclidean_paths
group by start_node_id),

closeness_centrality as (
SELECT
    split_part(relation_id,'|',2)::integer node_id,
    ROUND(AVG((value::numeric)),0) AS closeness
FROM
    networks.amm_network_length anp,
    jsonb_each_text(anp.data) AS kv
WHERE anp.relation_kind = 'length' and value::numeric < 100
GROUP BY
    split_part(relation_id,'|',2)),
    
centrality as (
    
select
c.node_id,
c.closeness,
round(s.straightness::numeric,4) straightness
--n.geometry
from
closeness_centrality c
join straightness_centrality s on c.node_id = s.node_id
--join networks.alcala_network_nodes n on b.node_id = n.node_id::integer
)
select * from centrality
--,



loaded_pois as (

select n.poi_id,c.*,p.housing,p.geometry 
from nearest_poi n
left join centrality c on n.node_id = c.node_id
left join pois p on n.poi_id = p.id
),

final_pois as (
select g.id tz_id,p.* from loaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

aggregated_tz as (
select 
tz_id,
ROUND(cast(SUM(betweeness * housing) / nullif(sum(housing),0) as NUMERIC),4) as mean_betweeness,
ROUND(cast(SUM(closeness * housing) / nullif(sum(housing),0) as NUMERIC),0) as mean_closeness,
ROUND(cast(SUM(straightness * housing) / nullif(sum(housing),0) as NUMERIC),4) as mean_straightness
from final_pois
group by tz_id)

select a.*,g.geometry from aggregated_tz a left join transportation_geo g on a.tz_id = g.id


--betweeness centrality
select
b.*,
(through_paths::float/all_paths::float) betweeness,
ann.geometry from networks.results_betweeness b join networks.alcala_network_nodes ann on b.node_id = ann.node_id 



drop table networks.temp_betweeness;
drop table networks.results_betweeness;

create table networks.temp_betweeness as 

WITH
nested_paths AS (
    SELECT
        split_part(relation_id, '|', 2)::integer AS start_node_id,
        kv.key::integer AS end_node_id,
        string_to_array(trim(both '[]' from kv.value),',')::int[] AS related_path
    FROM
        networks.alcala_network_paths anp,
        jsonb_each_text(anp.data) AS kv
    WHERE
        anp.relation_kind = 'path_nodes'
) 
,
ego_graphs AS (
    SELECT
        split_part(relation_id,'|',2)::integer AS node_id,
        string_to_array(trim(both '[]' from data ->> '900'),',')::int[] AS ego_900
    FROM
        networks.alcala_network_paths
    WHERE
        relation_kind = 'ego_graphs'
),

unnested_ego AS (
    SELECT node_id, cast(UNNEST(ego_900) as integer) as node_id_ego
    FROM ego_graphs
),

traversed as (
select
*,
case when node_id = any(related_path) then 1 else 0 end as traversed
from unnested_ego join nested_paths on node_id_ego = start_node_id)

select * from traversed;

CREATE INDEX ix_node_id ON networks.temp_betweeness(node_id);
CREATE INDEX ix_traversed ON networks.temp_betweeness(traversed);

create table networks.results_betweeness as 

SELECT
    node_id,
    COUNT(CASE WHEN traversed = 1 THEN 1 END) AS through_paths,
    COUNT(*) AS all_paths
FROM
    networks.temp_betweeness

GROUP BY
    node_id;

*/    
