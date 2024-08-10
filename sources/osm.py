#IMPORTS

from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass

import pandas as pd
import geopandas as gpd
from shapely import wkt, convex_hull
from shapely.geometry import MultiPoint, Point, Polygon, MultiPolygon
from loguru import logger

#GATHERER

nominatim = Nominatim()
overpass = Overpass()

def osm_frame(osm_query, type, color):
    id = []
    geometry = []
    osm_type = []
    tags = []

    force = {}
    for elemento in osm_query.elements():
        try: 
            elemento.geometry()
            force.update({elemento.id():False})
        except Exception:
            force.update({elemento.id():True})
            continue
    
    for elemento in osm_query.elements():
        if force[elemento.id()] == True:
            points = []
            data = get_rel_features(elemento.id())
            for a in data.elements():
                try:
                    if a.geometry()["type"] == "Polygon":
                        coords = a.geometry()["coordinates"]
                        for pol in coords:
                            for poi in pol:
                                point = Point([poi[0],poi[1]])
                                points.append(point) 
                    elif a.geometry()["type"] == "Point":
                        poi = a.geometry()["coordinates"]
                        point = Point([poi[0],poi[1]])
                        points.append(point) 

                except Exception:
                    continue

            g = convex_hull(MultiPoint(points))
            geometry.append(g)
            #print(g)
            id.append(elemento.id())
            osm_type.append(elemento.type())
            tags.append(elemento.tags())
            
        else:
            geometry.append(elemento.geometry())
            id.append(elemento.id())
            osm_type.append(elemento.type())
            tags.append(elemento.tags())

    d = {'id':id, 'geometry':geometry, 'type':osm_type, 'tags':tags, 'type':type, 'color':color}

    df = pd.DataFrame(data = d, index = d['id'])
    return df
    

def get_features(area : str, keywords:dict):
    q = (overpassQueryBuilder(
        area= area,
        elementType = keywords['elementType'],
        selector=keywords['selector'],
        out='body',
        includeGeometry=True))
    data = overpass.query(q)
    return data


def get_rel_features(rel_id):
    q = f"""
    (
    rel({rel_id});
    );
    (._;>;);
    out body geom;"""
    data = overpass.query(q)
    return data

    
def download_osm_data(path: str, areas: list, categories: dict):
    data = {}
    for area_name in areas:
        logger.info(f'Downloading OSM data for {area_name}...')
        area = nominatim.query(area_name, wkt=True)
        gdf = pd.concat(
            [gpd.GeoDataFrame(
                osm_frame(
                    get_features(
                        area,
                        categories[cat]),
                    cat,
                    categories[cat]['color'])
                ) for cat in categories]
            )
        
        data[area_name] = gdf
    
    for k,v in data.items():
        outdir = f"{path}\level0"
        out_data = r"{outdir}\level0_osm_{d}.csv".format(outdir=outdir,d=k)
        logger.info(f'Saving file in {out_data}...')
        v.to_csv(out_data,sep=";", index = False)


def json_data(data: pd.DataFrame, cols: list, idx: str) -> pd.Series:
    """
    Create JSON data from a DataFrame.

    Parameters:
        data (pd.DataFrame): The DataFrame to be converted.
        columns (List[str]): The columns to include in the JSON.
        idx (str): The column to use as the JSON index.

    Returns:
        A pandas Series with the JSON data.
    """
    try:
        dm = data.set_index(idx)[[c for c in cols if c in data.columns]].to_dict(orient='index')
        logger.info('Making JSON DATA columns...')
        return pd.Series(dm, index=data[idx])
    except Exception as e:
        logger.error(f"Failed to create JSON data. Error: {e}")
        return pd.Series()

def transform_osm(path: str, areas: list, provider: int):
    DATACOLS = ['type','area'] #,'tags','color'] TAGS NESTING IS NOT WORKING AS JSON. BEWARE
    for i, area_name in enumerate(areas):
        logger.info(f'Reading OSM data for {area_name}...')
        df = pd.read_csv(r"{path}\level0\level0_osm_{d}.csv".format(path=path,d=area_name),sep=";")
        logger.info(f'Making columns...')
        df['geometry'] = df.geometry.apply(wkt.loads)

        df = gpd.GeoDataFrame(df, geometry='geometry',crs=4326)
        df = df.to_crs(25830)
        df['area'] = df['geometry'].area
        df['provider'] = provider
        df['type'] = df['type'].str.replace("wood","park")
        df['type'] = df['type'].str.replace("forest","park")
        df['type'] = df['type'].str.replace("square","pedestrian")
        df['category'] = 'land use'+" - "+df['type']
        df['id'] = df['provider'].astype(str)+str(i)+"-"+df['id'].astype(int).astype(str).str.strip()

        df_points = df.loc[df.geometry.geom_type.isin(['MultiPoint','Point'])].drop_duplicates(subset='id').set_index('id',drop=False)
        df_polygons = df.loc[df.geometry.geom_type.isin(['MultiPolygon','Polygon'])].drop_duplicates(subset='id').set_index('id',drop=False)

        df_points['id_class'] = 'pois'
        data_points = json_data(df_points, DATACOLS, 'id' )
        df_points['data'] = data_points
        df_points = df_points[['id', 'id_class', 'category', 'provider', 'data','geometry']]

        df_polygons['geometry'] = df_polygons['geometry'].make_valid()

        for i,r in df_polygons.iterrows():
            if r['geometry'].geom_type == 'GeometryCollection':
                new_g = []
                gs = [g for g in r['geometry'].geoms if isinstance(g,Polygon) or isinstance(g,MultiPolygon)]
                for gg in gs:
                    if isinstance(gg,Polygon): 
                        new_g.append(gg)
                    else:
                        for p in gg.geoms:
                            new_g.append(p)
                df_polygons.at[i,'geometry'] = MultiPolygon(new_g)

        df_polygons = df_polygons[df_polygons['geometry'].is_valid]
        df_polygons = df_polygons.dissolve(by='category').reset_index(drop=False)
        df_polygons = df_polygons.explode().reset_index(drop=True)
        
        df_polygons['area'] = df_polygons['geometry'].area
        df_polygons['provider'] = provider
        df_polygons['id'] = df_polygons['provider'].astype(str)+str(i)+"-"+df_polygons.index.astype(str).str.strip()

        df_polygons['id_class'] = 'aois'
        df_polygons = df_polygons.set_index('id',drop=False)
        data_polygons = json_data(df_polygons, DATACOLS, 'id' )
        df_polygons['data'] = data_polygons
        df_polygons = df_polygons[['id', 'id_class', 'category', 'provider', 'data','geometry']]

        out_data_point = r"{path}\level2\level2_osm_{d}_point.csv".format(path=path,d=area_name)
        out_data_polygon = r"{path}\level2\level2_osm_{d}_polygon.csv".format(path=path,d=area_name)

        logger.info(f'\nSaving LEVEL2 data for {area_name}...')
        
        df_polygons.drop_duplicates(subset='id',inplace=True)
        df_points.drop_duplicates(subset='id',inplace=True)

        df_polygons.to_csv(out_data_polygon,sep=";",index=False)
        df_points.to_csv(out_data_point,sep=";",index=False)


def gather(source_instance, **kwargs):
    download_osm_data(path=kwargs.get('path'), areas= kwargs.get('areas'),  categories= kwargs.get('categories'))
def level0(source_instance, **kwargs):
    logger.warning('level0 is not needed in this source')
def level1(source_instance, **kwargs):
    transform_osm(path=kwargs.get('path'), areas= kwargs.get('areas'),provider=kwargs.get('provider'))
