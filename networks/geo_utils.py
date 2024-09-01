import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString, MultiPoint
from shapely.wkt import loads
from shapely.ops import linemerge, split
import rtree
from operator import itemgetter
from shapely.strtree import STRtree
import tqdm

def find_nearest(data: gpd.GeoDataFrame, data_near: gpd.GeoDataFrame, data_id: str, near_id: str):
    """
    Finds the nearest point in a point dataset to another point dataset
    Args:
        data (gpd.GeoDataFrame): The GeoDataFrame which will be updated with the nearest points IDs
        data_near (gpd.GeoDataFrame): The GeoDataFrame which contains the nearest points dataset
        data_id (str): The colum depicting the ID of the dataframe which will be updated
        near_id (str): The colum depicting the ID of the dataframe which will be tested for proximity

    Returns:
        gpd.GeoDataFrame: The updated input dataset, with a column containing the IDs of the nearest points
    """
    
    idx = rtree.index.Index()

    for fid, row in data_near.iterrows():
        geometry = row['geometry'].bounds
        idx.insert(fid, geometry)
    
    adjacency = {}
    
    #TODO vectorize this because is not performing great
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

def split_lines_at_points(data: gpd.GeoDataFrame, points: list, kept_attributes: list):
    """
    Splits a network dataset in a given list of points.
    Welds the remaining edges from their endpoints
    Note the points have to exist in the network dataset's edges
    Args:
        data (gpd.GeoDataFrame): A GeoDataFrame with the network edges
        points (list): A list of points where we want to split the network at
        kept_attributes (list): The list of attributes of the network dataset which we want to keep in the resulting dataset
        
    Returns:
        gpd.GeoDataFrame: The new network dataset split and welded using the input point list    
    """
    #Note that vertices have to exist in the geometry
    #points = MultiPoint([Point(loads(p)) for p in points])
    spatial_index = STRtree(points)

    mod_dataset = []
    for row in tqdm.tqdm(data.iterrows()):
        
        lin = row[1]['geometry']
 
        if lin.geom_type == 'MultiLineString' or lin is None:
        #if row[1]['geometry'].startswith('MULTILINESTRING Z'):
            continue
        #else:
            #lin = LineString(loads(row[1]['geometry']))
        line_attributes = row[1][kept_attributes].to_dict()
        coord = list(lin.coords)
        #Recalculates longitude and slope here TODO make this maybe more modular
        heights = (max(coord[0][2],coord[-1][2]),min(coord[0][2],coord[-1][2]))
        height = heights[0]-heights[1]
        long = lin.length
        slope = height/long

               # Use spatial index to find relevant points near the line
        potential_split_indices = list(spatial_index.query(lin))  # Ensure this returns a list
        potential_split_points = [points[i] for i in potential_split_indices] 

        inner = [p for p in potential_split_points if lin.distance(p) < 1]
        
        #inner = MultiPoint([Point(p) for p in coord[1:-1] if Point(p).distance(points) < 1])
            
        new_data = []

        if not inner:
            new_lines = [lin]
            pass
        else:
            new_lines = split(lin,MultiPoint(inner)).geoms
        for new_line in new_lines:
            attributes = line_attributes
            attr = {'slope':slope,'length':new_line.length,'geometry':new_line.wkt}
            attr.update(attributes)
            new_data.append(attr)

        mod_dataset.append(pd.DataFrame(new_data))

    split_df = pd.concat(mod_dataset)
    return(split_df)


def get_lines_endpoints(data: gpd.GeoDataFrame):
    """
    Returns the unique set of end points for each edge in the input network
    Args:
        data (gpd.GeoDataFrame): A GeoDataFrame with a network

    Returns:
        gpd.GeoDataFrame: A list of unique endpoints of the edges
    """
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


def remove_z_line(line: LineString):
    """
    Removes the Z coordinate from a Line object
    Args:
        line (LineString): A LineString object with X,Y,Z coordinates
    Returns:
        LineString: A LineString object with X,Y coordinates
    """
    #TODO Could be prepared for MultiLineString too
    #TODO Should be tested for non Z objects (try/except)
    return LineString([coordinate[:2] for coordinate in line.coords])

def remove_z_point(point: Point):
    """
    Removes the Z coordinate from a Point object
    Args:
        point (Point): A Point object with X,Y,Z coordinates
    Returns:
        Point: A Point object with X,Y coordinates
    """
    return Point(point.coords[0][:2])

def make_line(point1: Point, point2: Point):
    """
    Makes a Line object from two Point objects
    Args:
        point1 (Point): A Point object
        point2 (Point): A Point object

    Returns:
        LineString: A LineString object
    """
    return LineString([point1,point2])
