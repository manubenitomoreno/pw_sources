import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString, MultiPoint
from shapely.wkt import loads
from shapely.ops import linemerge, split
import rtree
from operator import itemgetter
import math

def find_nearest(data, data_near, data_id, near_id):
    
    idx = rtree.index.Index()

    for fid, row in data_near.iterrows():
        geometry = row['geometry'].bounds
        idf = row[near_id]
        idx.insert(fid, geometry)
        #idx.insert(idf, geometry)
    
    adjacency = {}
    
    for fid, row in data.iterrows():
        feature_id = row[data_id]
        bpoint = row['geometry'].bounds
        near_polys = {data_near.iloc[f][near_id]:data_near.iloc[f]['geometry'] for f in idx.nearest(bpoint, 1)}
        point = Point(row['geometry'])
        min_distance, min_poly = min(((poly.distance(point), poly) for poly in near_polys.values()), key=itemgetter(0))
        closest_poly = list({k:v for k,v in near_polys.items() if v == min_poly}.keys())[0]
        result = {feature_id:closest_poly}
        adjacency.update(result)
    adjacency = pd.DataFrame.from_dict(adjacency,orient='index',columns=[near_id])
    fdata = pd.merge(data.set_index(data_id),adjacency,how='left',left_index=True,right_index= True).reset_index()

    return fdata

def lines_and_interpolated_vertices(data,chunk,kept_attributes):
    
    mod_dataset = []
    re_mod_dataset = []
    for row in data.iterrows():
        lin = row[1]['geometry']
        line_attributes = row[1][kept_attributes].to_dict()
        vertices = MultiPoint(list(lin.coords))
        coord = tuple(lin.coords)
        
        segments ={}
        for position in range(len(coord)-1):
            segment = {position+1:LineString([Point(coord[position]),Point(coord[position+1])]).wkt}
            segments.update(segment)

        heights = (max(coord[0][2],coord[-1][2]),min(coord[0][2],coord[-1][2]))
        height = heights[0]-heights[1]
        start = [tup for tup in coord if heights[1] in tup][0]
        end = [tup for tup in coord if heights[0] in tup][0]
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
            
            result =[]
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
            #print("Not Broken")
        
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
    
    #The split dataset
    fin1 = pd.concat(mod_dataset)
    #the original dataset with added vertices
    fin2 = pd.concat(re_mod_dataset)
    return (fin1,fin2)
    
def split_lines_at_points(data,points,kept_attributes):
    #Note that vertices have to exist in the geometry
    points = MultiPoint([Point(loads(p)) for p in points])
    mod_dataset = []
    for row in data.iterrows():
        if row[1]['geometry'].startswith('MULTILINESTRING Z'):
            continue
        else:
            lin = LineString(loads(row[1]['geometry']))
        line_attributes = row[1][kept_attributes].to_dict()
        coord = list(lin.coords)
        heights = (max(coord[0][2],coord[-1][2]),min(coord[0][2],coord[-1][2]))
        height = heights[0]-heights[1]
        long = lin.length
        slope = height/long
        inner = MultiPoint([Point(p) for p in coord[1:-1] if Point(p).distance(points) < 1])
            
        new_data = []

        if inner.is_empty:
            new_lines = [lin]
            pass
        else:
            new_lines = split(lin,inner.geoms[0]).geoms
        for new_line in new_lines:
            attributes = line_attributes
            attr = {'slope':slope,'length':new_line.length,'geometry':new_line.wkt}
            attr.update(attributes)
            new_data.append(attr)

        mod_dataset.append(pd.DataFrame(new_data))

    split_df = pd.concat(mod_dataset)
    return(split_df)

def get_lines_endpoints(data):
    points = []
    for row in data.iterrows():
        try:
            lin = loads(row[1]['geometry'])
            l = list(lin.coords)
            ends = [l[0],l[-1]]
            points += ends
        except TypeError: print(row)
    points = list(dict.fromkeys(points))
    points = [Point(p) for p in points]
    return (points)

#Ahora estamos probando con 4 para ajustar mas
def tobler_speed_up(length,slope):
    #enters in radians and meters, gives minutes
    return round(4*(math.exp(-3.5*abs(slope+0.05)))*16.67,2)
def tobler_speed_down(length,slope):
    #enters in radians and meters, gives minutes   
    return round(4*(math.exp(-3.5*abs(-(slope)+0.05)))*16.67,2)

def calculate_speed_by_slope(network,length_attribute,slope_attribute):
    network['speed_up'] = network.apply(lambda row: tobler_speed_up (row[length_attribute],row[slope_attribute]),axis=1)
    network['speed_down'] = network.apply(lambda row: tobler_speed_down (row[length_attribute],row[slope_attribute]),axis=1)
    network['time_up'] = network[length_attribute] / network['speed_up']
    network['time_down'] = network[length_attribute] / network['speed_down']
    return(network)

def remove_z_line(data_z):
    for i,row in data_z.iterrows():
        a = [coordinate[:2] for coordinate in list(row['geometry'].coords)]
        data_z.at[i,'geometry'] = LineString(a)
    return data_z

def remove_z_point(data_z):
    for i,row in data_z.iterrows():
        #print(row['geometry'].coords[0][:2])
        data_z.at[i,'geometry'] = Point(row['geometry'].coords[0][:2])
    return data_z

def make_network_ids(edges):
    """
    
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
        

def make_network(network, pois, chunk_size=100):
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
    
    # Create measure points by splitting original network into specified chunks and retrieving all the resulting line intersections
    split_interpolated_network, modified_network = lines_and_interpolated_vertices(network, chunk_size, ['id'])
    measure_points = gpd.GeoDataFrame(
        get_lines_endpoints(split_interpolated_network),
        columns=['geometry'])
    measure_points['netpoint'] = measure_points.index
    
    # Measure points comprise both the new interpolated points and the original line endpoints
    # Calculate the nearest measure point to our set of pois
    nearest = find_nearest(pois, measure_points, 'id', 'netpoint')
    
    nearest = pd.merge(nearest, measure_points, how='left', on='netpoint')
    nearest['netpointg'] = nearest['geometry_y']
    nearest['originalg'] = nearest['geometry_x'].astype(str)
    nearest.drop(columns=['geometry_x', 'geometry_y'], inplace=True)
    nearest['geometry'] = nearest['netpointg']
    
    points_with_activity = [p.wkt for p in list(nearest.netpointg.unique())]
    
    original_endpoints = [p.wkt for p in get_lines_endpoints(network.to_wkt())]
    final_network_points_list = list(set(original_endpoints + points_with_activity))
    
    final_split_network = split_lines_at_points(modified_network, final_network_points_list, ['id'])
    final_split_network = calculate_speed_by_slope(final_split_network, 'length', 'slope')
    
    edges, nodes = make_network_ids(final_split_network)
    #final_network_points = pd.DataFrame(final_network_points_list).rename(columns={0: 'geometry'})
    #final_network_points['netpoint'] = final_network_points.index
    edges.drop_duplicates(subset=['node0_id','node1_id'],inplace=True)
    edges['edge_id'] = edges['id']
    
    
    nodes['geometry'] = nodes.apply(lambda row: Point(row['geometry']), axis=1)
    #final_network_points = gpd.GeoDataFrame(final_network_points, geometry='geometry')
    
    nearest = find_nearest(nearest[['id', 'originalg', 'geometry']], nodes, 'id', 'node_id')
    
    #final_network_points.rename(columns={'geometry': 'netpointg', 'netpoint_x': 'netpoint'}, inplace=True)
    #final_network_points = final_network_points[['netpoint', 'netpointg']]
    
    return edges, nearest, nodes
