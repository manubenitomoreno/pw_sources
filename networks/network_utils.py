import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString, MultiPoint
from shapely.wkt import loads
from shapely.ops import linemerge, split
import math
from loguru import logger


def lines_and_interpolated_vertices(data: gpd.GeoDataFrame, chunk: int, kept_attributes: list):
    """
    Breaks a linestring dataset representing a network into smaller chunks, wherever the edges are
    longer than the chunk size. Tries to divide these into the chunk size.
    Keeps the attributes passed
    Args:
        data (gpd.GeoDataFrame): The geodataframe of the road network
        chunk (int): The chunk size
        kept_attributes (list): The list of fields to keep

    Returns:
        tuple : The network split at all vertices, The original network, but the new interpolated vertices are added to the geometry
    """
    mod_dataset = []
    re_mod_dataset = []
    for row in data.iterrows():
        lin = row[1]['geometry']
        line_attributes = row[1][kept_attributes].to_dict()
        #vertices = MultiPoint(list(lin.coords))
        coord = tuple(lin.coords)
        
        segments ={}
        for position in range(len(coord)-1):
            segment = {position+1:LineString([Point(coord[position]),Point(coord[position+1])]).wkt}
            segments.update(segment)

        heights = (max(coord[0][2],coord[-1][2]),min(coord[0][2],coord[-1][2]))
        height = heights[0]-heights[1]
        #start = [tup for tup in coord if heights[1] in tup][0]
        #end = [tup for tup in coord if heights[0] in tup][0]
        long = lin.length
        slope = height/long
        if long < chunk+20:
            parts = 1
        if long >= chunk+20:
            parts = round(long/chunk)
        slice_dist = long / parts
        
        interpolated =[]
        if parts != 1:

            for i in range(parts):
                npoint = lin.interpolate(slice_dist*i)
                interpolated.append(npoint)
            interpolated = interpolated[1:]
            
            #result =[]
            for segment in segments:
                segment_line = LineString(loads(segments[segment]))
                segments[segment] = segment_line
                for p in interpolated:
                    if segment_line.distance(p) < 1e-8:
                        segments[segment] = LineString([Point(segment_line.coords[0]),p,Point(segment_line.coords[1])])
                    else:
                        continue     

            modified_line = MultiLineString([s for s in segments.values()])
            modified_line = linemerge (modified_line)
            breaks = MultiPoint(interpolated)
            new_lines = split(modified_line,breaks).geoms
            

        else:
            modified_line = lin
            new_lines = [modified_line]
        
        new_data = []
        mod_original = []
        for new_line in new_lines:
            attributes = line_attributes
            attr = {'slope':slope,'length':new_line.length,'geometry':new_line.wkt}
            attr.update(attributes)
            new_data.append(attr)

        attributes = line_attributes
        attr = {'slope':slope,'length':modified_line.length,'geometry':modified_line.wkt}
        attr.update(attributes)
        mod_original.append(attr)
        
        mod_dataset.append(pd.DataFrame(new_data))
        re_mod_dataset.append(pd.DataFrame(mod_original))
    
    #TODO make this more explicit
    #The split dataset
    fin1 = pd.concat(mod_dataset)
    #the original dataset with added vertices
    fin2 = pd.concat(re_mod_dataset)
    return (fin1,fin2)


#TODO an average speed of 4km/h is currently hard-coded. Consider parametrizing this to enable different pedestrian modes
def tobler_speed_up(length,slope):
    """
    Calculates up slope speed according to Tobler's Hitchiker equation
    Args:
        length (float): The length of the edge
        slope (float): The slope of the edge

    Returns:
        float: The speed considering up slope
    """
    #enters in radians and meters, gives minutes
    return round(4*(math.exp(-3.5*abs(slope+0.05)))*16.67,2)
def tobler_speed_down(length,slope):
    """
    Calculates up slope speed according to Tobler's Hitchiker equation
    Args:
        length (float): The length of the edge
        slope (float): The slope of the edge

    Returns:
        float: The speed considering up slope
    """
    #enters in radians and meters, gives minutes   
    return round(4*(math.exp(-3.5*abs(-(slope)+0.05)))*16.67,2)

def calculate_speed_by_slope(network: gpd.GeoDataFrame, length_attribute: str, slope_attribute: str):
    """
    Calculates the speed and time attributes for a network geodataframe, using tobler's hitchiker equation
    Args:
        network (gpd.GeoDataFrame): The input network
        length_attribute (_type_): The column with the length values, in meters
        slope_attribute (_type_): The column with the slope values, in meters
    Returns:
        gpd.GeoDataFrame: The updated GeoDataFrame
    """
    network['speed_up'] = network.apply(lambda row: tobler_speed_up (row[length_attribute],row[slope_attribute]),axis=1)
    network['speed_down'] = network.apply(lambda row: tobler_speed_down (row[length_attribute],row[slope_attribute]),axis=1)
    network['time_up'] = network[length_attribute] / network['speed_up']
    network['time_down'] = network[length_attribute] / network['speed_down']
    return(network)


def make_network_ids(edges: gpd.GeoDataFrame):
    """
    Makes consistent edges and nodes IDs and extracts both datasets from a network dataset
    Args:
        edges (gpd.GeoDataFrame): The GeoDataFrame containing the edges
    Returns:
        gpd.GeoDataFrame, gpd.GeoDataFrame: A tuple with both GeoDataFrames, edges and nodes, labelled accordingly to the network they pertain to
    """
    nodes = {}
    edges_nodes = {}
    edges.insert(0, 'edge_id', range(0,len(edges)))
    node_id = 0

    for i, row in edges.iterrows():
        lin = loads(row['geometry'])
        l = list(lin.coords)
        ends = [l[0],l[-1]]
        edges_nodes.update({row['edge_id'] : ends})

        for end in ends:
            if end in nodes.keys():
                pass
            else:
                node_id += 1
                nodes.update({end:node_id})
                
    edges['node0'] = edges['edge_id'].map({k:v[0] for k,v in edges_nodes.items()})
    edges['node1'] = edges['edge_id'].map({k:v[1] for k,v in edges_nodes.items()})
    
    edges['node0_id'] = edges['node0'].map(nodes)
    edges['node1_id'] = edges['node1'].map(nodes)
    nodes = pd.DataFrame(nodes.items(), columns=['geometry', 'node_id'])
    return edges, nodes
        