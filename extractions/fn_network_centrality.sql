

with

nodes_degree as (
SELECT
    an.node_id node_id,
    cast(an.data ->> 'degree' as FLOAT) AS node_degree
FROM
    networks.amm_network_nodes an),

ego_graphs as 
(select cast(split_part(relation_id,'|',2) as integer) node_id,
string_to_array(trim(both '[]' from data ->> '900'),',') as ego
from networks.amm_network_ego where relation_kind = 'ego_graphs'),

unnested_ego AS (
    SELECT node_id, cast(UNNEST(ego) as integer) as reachable_node_id
    FROM ego_graphs
),

average_degree as (
SELECT unnested_ego.node_id,
AVG(node_degree) node_degree
FROM unnested_ego
JOIN nodes_degree ON unnested_ego.reachable_node_id = nodes_degree.node_id 
GROUP BY unnested_ego.node_id),

transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone'),

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
        anp.relation_kind = 'length' and kv.value::float < 300
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

centrality as (
    
select
d.node_id,
d.node_degree des_mean_degree,
round(s.straightness::numeric,4) des_straightness
from
average_degree d
join straightness_centrality s on d.node_id = s.node_id
),

loaded_pois as (

select n.poi_id,c.*,p.housing,p.geometry 
from nearest_poi n
left join centrality c on n.node_id = c.node_id
left join pois p on n.poi_id = p.id
),

final_pois as (
select g.id tz_id,p.* from loaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
)
select tz_id,poi_id,des_mean_degree,des_straightness,geometry from final_pois
