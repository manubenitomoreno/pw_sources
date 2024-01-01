--delete from networks.alcala_network_edges;
--delete from networks.alcala_network_nodes;
--delete from networks.alcala_network_paths;
--delete from networks.alcala_network_relations;
--select * from networks.alcala_network_edges ane limit 10



with

transportation_geo as (select * from sources.boundaries_geo bg where category = 'boundary-transportation_zone'),

pois as (
select id, 
cast(data ->> 'Origins - Housing - Single Family Residence - Number' as float)
+ 
cast(data ->> 'Origins - Housing - Collective Housing - Number' as float) as housing,
geometry
from sources.pois where category = 'land use - general'
),

nearest_poi as (
select
data ->> 'poi_id' poi_id,
cast(data ->> 'node_id' as integer) node_id
from networks.alcala_network_relations anr where relation_kind = 'nearest_poi'),

edges as (
select edge_id,"start" associated_node,
cast(data ->> 'original_id' as integer) original_id,
case when cast(data ->> 'culdesac_length' as float) = 0.0 then 0 else 1 end as culdesacs,
geometry
from networks.alcala_network_edges
union all
select edge_id,"end" associated_node,
cast(data ->> 'original_id' as integer) original_id,
case when cast(data ->> 'culdesac_length' as float) = 0.0 then 0 else 1 end as culdesacs,
geometry
from networks.alcala_network_edges
),

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

ego_edges as (

select distinct node_id, edge_id
from unnested_ego u join edges e on u.node_id_ego = e.associated_node),

ego_streets as (
select eg.*,ed.culdesacs,ceil(st_length(ed.geometry)) edge_length from ego_edges eg join 
(select edge_id,
case when cast(data ->> 'culdesac_length' as float) = 0.0 then 0 else 1 end as culdesacs,
geometry
from networks.alcala_network_edges) ed
on eg.edge_id = ed.edge_id),

node_culdesacs as (
select
node_id,
sum(edge_length) filter (where culdesacs = 1) / sum(edge_length) ratio_culdesacs
from ego_streets group by node_id),

loaded_pois as (

select n.poi_id,c.*,
coalesce(nullif (p.housing, 'NaN'),0) housing,
p.geometry 
from nearest_poi n
left join node_culdesacs c on n.node_id = c.node_id
left join pois p on n.poi_id = p.id
),

final_pois as (
select g.id tz_id,p.* from loaded_pois p, transportation_geo g where st_contains(g.geometry,p.geometry)
),

aggregated_tz as (
select 
tz_id,
ROUND(cast(SUM(ratio_culdesacs * housing) / nullif(sum(housing),0) as NUMERIC),2) as mean_ratio_culdesacs
from final_pois
group by tz_id)

select a.*,g.geometry from aggregated_tz a left join transportation_geo g on a.tz_id = g.id