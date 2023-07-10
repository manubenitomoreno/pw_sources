  
import requests, zipfile, io, os
import geopandas as gpd
import pandas as pd
"""
============================================================================
============================================================================
GATHERER FOR INE GEO SOURCE - SPAIN
============================================================================
CONTRIBUITORS: MANU BENITO
============================================================================
"""  
def download_ine_geo_data(codes, path):
    
    for code in codes:
        URL = f"https://www.ine.es/prodyser/cartografia/seccionado_{code}.zip"
        r = requests.get(URL)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(f"{path}\level0")
"""
============================================================================
NO LEVEL0 NEEDED FOR INE GEO SOURCE - SPAIN
============================================================================

============================================================================
============================================================================
LEVEL1 FOR INE GEO SOURCE - SPAIN
============================================================================
CONTRIBUITORS: MANU BENITO
============================================================================
TRANSFORM DATA INTO WALKNET FORMATS
"""

def open_ine_geo_data(path, codes):
    files = {}
    for code in codes:
        files.update({code:gpd.read_file([f"{path}\level0\España_Seccionado{code}_ETRS89H30\{c}" for c in os.listdir(f"{path}\level0\España_Seccionado{code}_ETRS89H30") if '.shp' in c and code in c][0])})
    return files

def process_ine_geo_layer(path, code, gdf):
    
    fields = ['province_code','municipality_code','district_code','census_tract_code','province_name','municipality_name','district_name','census_tract_name','boundary_code','boundary_name','boundary_source','boundary_period', 'boundary_type','geometry']
    
    gdf['boundary_code'] = gdf['CUSEC']
    gdf['boundary_name'] = f"Sección "+gdf['CDIS']+gdf['CSEC']
    gdf['boundary_source'] = 'Instituto Nacional de Estadística'
    gdf['boundary_period'] = code
    gdf['boundary_type'] = 'census_tract'
    gdf['census_tract_code'] = gdf['CUSEC']
    gdf['census_tract_name'] = f"Sección "+gdf['CDIS']+gdf['CSEC']
    gdf['district_code'] = gdf['CUDIS']
    gdf['district_name'] = f"Distrito "+gdf['CDIS']
    gdf['municipality_code'] = gdf['CUMUN']
    gdf['municipality_name'] = gdf['NMUN']
    gdf['province_code'] = gdf['CPRO']
    gdf['province_name'] = gdf['NPRO']
    
    gdf = gdf[fields]
    
    gdf = gdf.to_crs('epsg:4326')
    
    district = gdf.dissolve(by=['province_code','municipality_code','district_code','province_name','municipality_name','district_name']).reset_index()
    
    district['boundary_code'] = district['district_code']
    district['boundary_name'] = district['district_name']
    district['boundary_source'] = 'Instituto Nacional de Estadística'
    district['boundary_period'] = code
    district['boundary_type'] = 'district'
    district['census_tract_code'] = ''
    district['census_tract_name'] = ''
    
    district = district[fields]
    
    municipality = district.dissolve(by=['province_code','municipality_code','province_name','municipality_name']).reset_index()
    
    municipality['boundary_code'] = district['municipality_code']
    municipality['boundary_name'] = district['municipality_name']
    municipality['boundary_source'] = 'Instituto Nacional de Estadística'
    municipality['boundary_period'] = code
    municipality['boundary_type'] = 'municipality'
    municipality['district_code'] = ''
    municipality['district_name'] = ''
    
    municipality = municipality[fields]
    
    province = municipality.dissolve(by=['province_code','province_name']).reset_index()
    
    province['boundary_code'] = province['province_code']
    province['boundary_name'] = province['province_name']
    province['boundary_source'] = 'Instituto Nacional de Estadística'
    province['boundary_period'] = code
    province['boundary_type'] = 'province'
    province['municipality_code'] = ''
    province['municipality_name'] = ''
    
    province = province[fields]
    
    pd.concat([gdf,district,municipality,province]).to_file(f"{path}\level1\level1_ine_geo_{code}.gpkg", driver = 'GPKG')
    
def process_ine_geo_data(codes, path):
    files = open_ine_geo_data(path, codes)
    for code,gdf in files.items():
        process_ine_geo_layer(path, code, gdf)
            
        
def gather(path: str, codes:list):
    download_ine_geo_data(codes, path)
    #logging.DEBUG('Gathering data for...')
def level1(path: str, codes:list):
    process_ine_geo_data(codes, path)
    #logging.INFO('Processing level1 for...')
#def persist():
    #logging.INFO('Persisting data for...')
