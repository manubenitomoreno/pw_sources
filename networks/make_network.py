import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.wkt import loads
import json

from networks.geo_utils import * 
from networks.network_utils import *
    
def json_data(row, columns, id_col):
    """ 
    Just a simple function to convert a list of columns into a JSON column
    """
    data = {col: row[col] for col in columns if col in row.index}
    data['id'] = row[id_col]  # Add the ID to the JSON data
    return json.dumps(data)

def make_network(network: gpd.GeoDataFrame, pois: gpd.GeoDataFrame, chunk_size=100):
    """
    Creates a network by splitting the original network and associating points of interest (POIs).

    Parameters:
    - network: Geopandas DataFrame of the road network.
    - pois: Geopandas DataFrame of points of interest.
    - chunk_size: distance at which the network will be split. Default is 100 meters.

    Returns:
    - final_split_network: Geopandas DataFrame of the split network.
    - nearest: DataFrame indicating the nearest network points for each POI.
    - final_network_points: DataFrame of the final network points.
    """
    logger.info(f"Interpolating intermediate vertices")
    # Create measure points by splitting original network into specified chunks and retrieving all the resulting line intersections
    split_interpolated_network, modified_network = lines_and_interpolated_vertices(
        network,
        chunk_size,
        ['id'])
    logger.info(f"Setting possible measure points (points near a POI)")
    measure_points = gpd.GeoDataFrame(
        get_lines_endpoints(split_interpolated_network),
        columns=['geometry'])
    
    measure_points['netpoint'] = measure_points.index
    
    # Measure points comprise both the new interpolated points and the original line endpoints
    
    # Calculate the nearest measure point to our set of pois
    nearest = find_nearest(
        pois,
        measure_points,
        'id',
        'netpoint')
    
    nearest = pd.merge(
        nearest,
        measure_points,
        how='left',
        on='netpoint')
    
    # Rename the columns for clarity
    nearest['netpointg'] = nearest['geometry_y']
    nearest['originalg'] = nearest['geometry_x'].astype(str)
    nearest.drop(columns=['geometry_x', 'geometry_y'], inplace=True)
    nearest['geometry'] = nearest['netpointg']
    
    # Final points for our network are the original endpoints of edges, and those interpolated points that have a nearby POI
    points_with_activity = [p.wkt for p in list(nearest.netpointg.unique())]
    original_endpoints = [p.wkt for p in get_lines_endpoints(network.to_wkt())]
    final_network_points_list = list(set(original_endpoints + points_with_activity))
    
    logger.info(f"Splitting network at relevant vertices")
    #These final points are passed to the network to be split at their location, welding the rest of edges
    final_split_network = split_lines_at_points(
        modified_network,
        final_network_points_list,
        ['id'])
    
    logger.info(f"Calculating slope and speed")
    #We then calculate speed, yet we could calculate other attributes here
    final_split_network = calculate_speed_by_slope(final_split_network, 'length', 'slope') #TODO please write some tests for this
    
    logger.info(f"Making network IDs")
    #Finally, we drop some geometric duplicates (TODO it would be nice to understand why they exist)
    #And extract coherently labelled edges and nodes
    final_split_network.drop_duplicates(subset=['geometry'],inplace=True)
    edges, nodes = make_network_ids(final_split_network) 

    logger.info(f"Producing nodes")
    #Some geometric treatment for our nodes, these would be interesting to include inside make_network_ids function TODO
    #The removal of Z point is necessary for POSTGIS management, it does not support Z coords
    nodes['geometry'] = nodes.apply(lambda x: Point(x['geometry']), axis=1)
    nodes['geometry'] = nodes.apply(lambda x: remove_z_point(x['geometry']),axis=1)
    
    #Now we calculate nearest POIs to our final nodes again. TODO maybe this could be simplified but it is not critical
    #TODO again, the process yields duplicates somehow
    nearest = find_nearest(nearest[['id', 'originalg', 'geometry']], nodes, 'id', 'node_id')
    nearest.drop_duplicates(inplace=True)
    
    #Final treatment of our nodes
    nodes = gpd.GeoDataFrame(nodes)
    nodes = nodes[['node_id','geometry']]
    
    logger.info(f"Producing edges")
    #Final treatment of our edges
    edges['data'] = edges.apply(lambda row : json_data(row, ['slope','length','speed_up','speed_down','time_up','time_down'], 'edge_id'),axis=1)
    edges.rename(columns={'node0_id':'start','node1_id':'end'},inplace=True)
    edges['geometry'] = edges['geometry'].apply(loads)
    edges['geometry'] = edges['geometry'].apply(lambda x: remove_z_line(x))
    edges = gpd.GeoDataFrame(edges,geometry='geometry')
    edges = edges[['edge_id','data','start','end','geometry']]
    
    logger.info(f"Producing relations")
    #Final treatment of our relations table (in this process, merely relation with nearest POIs)
    nearest['relation_id'] = nearest['index']+"|"+nearest['node_id'].astype(str)
    nearest['relation_kind'] = 'nearest_poi'
    nearest['poi_id'] = nearest['index']
    
    #I have included a simple line to visualize the relation of the nodes and the POIs
    nearest['geometry'] = nearest.apply(lambda x: remove_z_point(x['geometry']),axis=1)
    nearest['originalg'] = nearest['originalg'].apply(loads)
    nearest['geometry'] = nearest.apply(lambda x: make_line(x['originalg'], x['geometry']),axis=1)
    nearest['geometry'] = nearest.apply(lambda x: x['geometry'].wkt, axis=1)
    nearest['data'] = nearest.apply(lambda row: json_data(row,['poi_id','node_id','geometry'],'index'),axis=1)
    nearest = nearest[['relation_id','relation_kind','data','geometry']]
    
    return edges, nearest, nodes
