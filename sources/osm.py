#IMPORTS

from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass

import pandas as pd
import geopandas as gpd

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
        v.to_csv(out_data,sep=";", index = False)

def json_data(data, columns, idx):
    d =  pd.Series(data.set_index(idx)[[c for c in columns if c in data.columns]].to_dict(orient='records'))
    d = d.astype(str).str.replace("'",'"')
    #logging.info(f'Making JSON DATA columns...')
    return d

def transform_osm(path: str, areas: list, provider: int):
    DATACOLS = ['type','tags','color']
    for area in areas:
        df = pd.read_csv(r"{path}\level0\level0_osm_{d}.csv".format(path=path,d=area),sep=";")
        df['provider'] = provider
        df['category'] = 'land use'+" - "+df['type']
        df['class'] = 'pois'
        df['data'] = json_data(df, DATACOLS, 'id' )
        df['id'] = df['provider'].astype(str)+"-"+df['id'].astype(int).astype(str)
        df = df[['id', 'class', 'category', 'provider', 'data','geometry']]
        df.to_csv(r"{path}\level2\level2_osm_{d}.csv".format(path=path,d=area),sep=";",index=False)

def gather(source_instance, **kwargs):
    download_osm_data(path=kwargs.get('path'), areas= kwargs.get('areas'),  categories= kwargs.get('categories'))
def level0(source_instance, **kwargs):
    logger.warning('level0 is not needed in this source')
def level1(source_instance, **kwargs):
    transform_osm(path=kwargs.get('path'), areas= kwargs.get('areas'),provider=kwargs.get('provider'))
