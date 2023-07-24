#IMPORTS

from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass

import pandas as pd
import geopandas as gpd

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
        out_data = r"{outdir}\level0_osm_{d}.parquet".format(outdir=outdir,d=k)
        print(v)
        v.to_parquet(out_data)

def gather(source_instance, **kwargs):
    download_osm_data(path=kwargs.get('path'), areas= kwargs.get('areas'),  categories= kwargs.get('categories'))
#def level0(path: str, codes:list):
    #process_cadastral_data(path, codes)
    #logging.INFO('Processing level0 for...')
#def level1(path: str):
    #process_edm_data(path, files, schemas)
    #logging.INFO('Processing level1 for...')
#def persist():
    #logging.INFO('Persisting data for...')