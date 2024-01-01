#IMPORTS

from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import MultiPoint
from loguru import logger

#GATHERER

nominatim = Nominatim()
overpass = Overpass()

def osm_frame(osm_query, type, color):
    id = []
    geometry = []
    osm_type = []
    tags = []

    for elemento in osm_query.elements():
        id.append(elemento.id())
        geometry.append(elemento.geometry())
        osm_type.append(elemento.type())
        tags.append(elemento.tags())
        
    d = {'id':id, 'geometry':geometry, 'type':osm_type, 'tags':tags, 'type':type, 'color':color}

    df = pd.DataFrame(data = d, index = d['id'])
    return df

def get_features(area : str, keywords:dict):
    return overpass.query(overpassQueryBuilder(
        area= area,
        elementType = keywords['elementType'],
        selector=keywords['selector'],
        out='body',
        includeGeometry=True))
    
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


def json_data(data: pd.DataFrame, columns: list, idx: str) -> pd.Series:
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
        d = data.set_index(idx)[[c for c in columns if c in data.columns]].to_dict(orient='records')
        logger.info('Making JSON DATA columns...')
        return pd.Series(d)
    except Exception as e:
        logger.error(f"Failed to create JSON data. Error: {e}")
        return pd.Series()


def transform_osm(path: str, areas: list, provider: int):
    DATACOLS = ['type'] #,'tags','color'] TAGS NESTING IS NOT WORKING AS JSON. BEWARE
    for area_name in areas:
        logger.info(f'Reading OSM data for {area_name}...')
        df = pd.read_csv(r"{path}\level0\level0_osm_{d}.csv".format(path=path,d=area_name),sep=";")
        logger.info(f'Making columns...')
        df['geometry'] = df.geometry.apply(wkt.loads)
        df = gpd.GeoDataFrame(df, geometry='geometry',crs=4326)
        df = df.to_crs(25830)
        df['provider'] = provider
        df['category'] = 'land use'+" - "+df['type']
        df['id_class'] = 'pois'
        df['data'] = json_data(df, DATACOLS, 'id' )
        df['id'] = df['provider'].astype(str)+"-"+df['id'].astype(int).astype(str).str.strip()
        df.drop_duplicates(subset='id',inplace=True)
        df.loc[~df.geometry.geom_type.isin(['MultiPoint','Point']),'geometry'] = df['geometry'].centroid
        df = df[['id', 'id_class', 'category', 'provider', 'data','geometry']]
        out_data = r"{path}\level2\level2_osm_{d}.csv".format(path=path,d=area_name)
        logger.info(f'\nSaving LEVEL2 data for {area_name} in:\n {out_data}...')
        df.to_csv(out_data,sep=";",index=False)


def gather(source_instance, **kwargs):
    download_osm_data(path=kwargs.get('path'), areas= kwargs.get('areas'),  categories= kwargs.get('categories'))
def level0(source_instance, **kwargs):
    logger.warning('level0 is not needed in this source')
def level1(source_instance, **kwargs):
    transform_osm(path=kwargs.get('path'), areas= kwargs.get('areas'),provider=kwargs.get('provider'))
