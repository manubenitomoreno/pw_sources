  
import requests, zipfile, io, os
import geopandas as gpd
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry
import pandas as pd
from loguru import logger
from typing import List, Dict, Any
import json

def download_ine_geo_data(years: List[int], path: str) -> None:
    """
    Download INE geo data for each year and extract it to a specified path.

    Parameters:
        years (List[int]): The years for which to download the data.
        path (str): The path where the downloaded data should be extracted.
    """
    for year in years:
        try:
            logger.info(f'Reading INE geo data URL for {year}...')
            URL = f"https://www.ine.es/prodyser/cartografia/seccionado_{year}.zip"
            r = requests.get(URL)
            r.raise_for_status()  # This will raise an HTTPError if the request failed
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                z.extractall(os.path.join(path, "level0"))
                logger.info(f'Extracted INE geo data ZIP for {year}.')
        except Exception as e:
            logger.error(f"Failed to download or extract data for year {year}. Error: {e}")

def open_ine_geo_data(path: str, years: List[int]) -> Dict[int, gpd.GeoDataFrame]:
    """
    Open INE geo data for each year.

    Parameters:
        path (str): The path where the data is located.
        years (List[int]): The years for which to open the data.

    Returns:
        A dictionary mapping each year to its corresponding GeoDataFrame.
    """
    files = {}
    for year in years:
        try:
            year_folder = os.path.join(path, "level0", f"España_Seccionado{year}_ETRS89H30")
            shp_files = [os.path.join(year_folder, c) for c in os.listdir(year_folder) if '.shp' in c and str(year) in c]
            files[year] = gpd.read_file(shp_files[0])
            logger.info(f'Successfully read geo data for {year}.')
        except Exception as e:
            logger.error(f"Failed to read geo data for year {year}. Error: {e}")
    return files

def process_ine_geo_layer(year: int, gdf: gpd.GeoDataFrame, path: str) -> None:
    """
    Process a geo data layer for a given year.

    Parameters:
        year (int): The year of the data.
        gdf (gpd.GeoDataFrame): The GeoDataFrame to be processed.
        path (str): The path where the processed data should be saved.
    """
    try:    
        fields = ['province_code','municipality_code','district_code','census_tract_code','province_name','municipality_name','district_name','census_tract_name','boundary_code','boundary_name','boundary_source','boundary_period', 'boundary_type','geometry']
        logger.info(f'Make INE  nested official codes')
        gdf['boundary_code'] = gdf['CUSEC']
        gdf['boundary_name'] = ""#f"Sección "+gdf['CDIS']+gdf['CSEC']
        gdf['boundary_source'] = ""#'Instituto Nacional de Estadística'
        gdf['boundary_period'] = year
        gdf['boundary_type'] = 'census_tract'
        gdf['census_tract_code'] = gdf['CUSEC']
        gdf['census_tract_name'] = ""#f"Sección "+gdf['CDIS']+gdf['CSEC']
        gdf['district_code'] = gdf['CUDIS']
        gdf['district_name'] = ""#f"Distrito "+gdf['CDIS']
        gdf['municipality_code'] = gdf['CUMUN']
        gdf['municipality_name'] = ""#gdf['NMUN']
        gdf['province_code'] = gdf['CPRO']
        gdf['province_name'] = ""#gdf['NPRO']
        
        gdf = gdf[fields]
        logger.info(f'Reproject the layer')
        gdf = gdf.to_crs('epsg:4326')
        logger.info(f'Dissolve the layer into other levels')
        
        district = gdf.dissolve(by=['province_code','municipality_code','district_code','province_name','municipality_name','district_name']).reset_index()
        district['boundary_code'] = district['district_code']
        district['boundary_name'] = district['district_name']
        district['boundary_source'] = ""#'Instituto Nacional de Estadística'
        district['boundary_period'] = year
        district['boundary_type'] = 'district'
        district['census_tract_code'] = ''
        district['census_tract_name'] = ''
        district = district[fields]
    
        municipality = district.dissolve(by=['province_code','municipality_code','province_name','municipality_name']).reset_index()
        municipality['boundary_code'] = district['municipality_code']
        municipality['boundary_name'] = district['municipality_name']
        municipality['boundary_source'] = ""#'Instituto Nacional de Estadística'
        municipality['boundary_period'] = year
        municipality['boundary_type'] = 'municipality'
        municipality['district_code'] = ''
        municipality['district_name'] = ''
        municipality = municipality[fields]
        
        province = municipality.dissolve(by=['province_code','province_name']).reset_index()
        province['boundary_code'] = province['province_code']
        province['boundary_name'] = province['province_name']
        province['boundary_source'] = ""#'Instituto Nacional de Estadística'
        province['boundary_period'] = year
        province['boundary_type'] = 'province'
        province['municipality_code'] = ''
        province['municipality_name'] = ''
    
        province = province[fields]
        final = pd.concat([gdf,district,municipality,province])
        out_dir = f"{path}\level1\level1_ine_geo_{year}.gpkg"
        logger.info(f'Concatenate the results and save file into:\n{out_dir}')
        final.to_file(out_dir, driver = 'GPKG')
        
    except Exception as e:
        logger.error(f"Failed to process geo data for year {year}. Error: {e}")
    
def process_ine_geo_data(years: List[int], path: str) -> None:
    """
    Process INE geo data for each year.

    Parameters:
        years (List[int]): The years for which to process the data.
        path (str): The path where the processed data should be saved.
    """
    try:
        files = open_ine_geo_data(path, years)
        for year, gdf in files.items():
            process_ine_geo_layer(year, gdf, path)
    except Exception as e:
        logger.error(f"Failed to process geo data. Error: {e}")
       

def json_data(data: pd.DataFrame, columns: List[str], idx: str) -> pd.Series:
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
    
def transform_ine_geo_data(path: str, years: List[int], provider: str) -> None:
    """
    Transform INE geo data for each year.

    Parameters:
        path (str): The path where the data is located.
        years (List[int]): The years for which to transform the data.
        provider (str): The provider of the data.
    """
    try:
        DATACOLS = ['area', 'province_code', 'municipality_code', 'district_code', 'census_tract_code', 'province_name', 'municipality_name', 'district_name', 'census_tract_name']
        for year in years:
            logger.info(f'Reading INE geo data for {year}...')
            year_path = os.path.join(path, "level1", f"level1_ine_geo_{year}.gpkg")
            gdf = gpd.read_file(year_path)
            gdf['provider'] = provider
            logger.info(f'Making columns...')
            gdf['id'] = gdf['provider'].astype(str)+"-"+year+"-"+gdf['boundary_code'].str.strip()
            gdf['id_class'] = 'boundary'
            gdf['data'] = json_data(gdf, DATACOLS, 'id' )
            gdf['category'] = 'boundary-'+gdf['boundary_type']
            logger.info(f'Forcing MultiPolygon...')
            gdf['geometry'] = gdf.apply(lambda row: force_multypolygon(row['geometry']),axis=1)
            gdf = gdf[['id', 'id_class', 'category', 'provider', 'data','geometry']]
            gdf.drop_duplicates(subset='id',inplace=True)
            out_data = os.path.join(path, "level2", f"level2_ine_geo_{year}.csv")
            logger.info(f'\nSaving LEVEL2 data for {year} in:\n {out_data}...')
            gdf.to_csv(out_data,sep=";",index=False)
    except Exception as e:
        logger.error(f"Failed to transform geo data. Error: {e}")

def gather(source_instance, **kwargs):
    download_ine_geo_data(years = kwargs.get('years'), path = kwargs.get('path'))
def level0(source_instance, **kwargs):
    process_ine_geo_data(years = kwargs.get('years'), path = kwargs.get('path'))
def level1(source_instance, **kwargs):
    transform_ine_geo_data(path = kwargs.get('path'),years = kwargs.get('years'),provider = kwargs.get('provider'))
