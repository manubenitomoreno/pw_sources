  
import os
import geopandas as gpd
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry
import pandas as pd
from loguru import logger
from typing import List, Dict, Any
import json

def force_multypolygon(geometry: BaseGeometry) -> BaseGeometry:
    """
    Convert a Polygon geometry to a MultiPolygon if necessary.

    Parameters:
        geometry (BaseGeometry): The geometry to be converted.

    Returns:
        The converted geometry.
    """
    try:
        if geometry.geom_type == 'Polygon':
            return MultiPolygon([geometry])
        else:
            return geometry
    except Exception as e:
        logger.error(f"Failed to transform geometry. Error: {e}")
        return geometry


def process_edm_geo_layer(path: str, provider: int) -> None:
    """
    Process the geo data layer for EDM.

    Parameters:
        
        gdf (gpd.GeoDataFrame): The GeoDataFrame to be processed.
        path (str): The path where the processed data should be saved.
    """
    try:
        gdf = gpd.read_file(f"{path}\level0\ZonificacionZT1259.geojson",crs = 4326)
        gdf = gdf.to_crs(25830)
        gdf['id'] = gdf['ZT1259']
        gdf['id_class'] = "boundary"
        gdf['category'] = "boundary-transportation_zone"
        gdf['provider'] = provider
        gdf['data'] = '{"":""}'
        gdf = gdf[['id','id_class','category','provider','data','geometry']]
        gdf['geometry'] = gdf.apply(lambda row: force_multypolygon(row['geometry']),axis=1)
        out_data = os.path.join(path, "level2", f"level2_edm_geo.csv")
        logger.info(f'\nSaving LEVEL2 data in:\n {out_data}...')
        gdf.to_csv(out_data,sep=";",index=False)
        
    except Exception as e:
        logger.error(f"Failed to process geo data")

def gather(source_instance, **kwargs):
    logger.error(f"Source does not need a gather function. Please download it manually")
    return
def level0(source_instance, **kwargs):
    logger.error(f"Source does not need a level0 function.")
    return
def level1(source_instance, **kwargs):
    process_edm_geo_layer(path = kwargs.get('path'),provider = kwargs.get('provider'))
