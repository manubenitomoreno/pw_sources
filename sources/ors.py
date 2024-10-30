import pandas as pd
import os
from shapely.wkt import loads
import geopandas as gpd
from shapely.geometry import Point, Polygon
import openrouteservice as ors
from loguru import logger
from tqdm import tqdm
import json
from datetime import date, datetime
from ast import literal_eval
import itertools

network = 'amm_network'
DATALAKE_PATH = "G:\Mi unidad\walknet_datalake"

def storage_file(path, network):
    # Open json file and read into dict

    with open(r"{path}\level0\level0_{network}.json".format(path = path, network=network),) as f:
        
        data = json.load(f)
        data = json.loads(data)

        data = {datetime.strptime(date, '%d/%m/%Y').date():{literal_eval(c): loads(p) for c,p in calls.items()} for date, calls in data.items()}
        return data
    
def make_measure_points(network, datalake_path, round_to = 2):
    nodes = pd.concat([pd.read_csv(
        r"{datalake_path}\networks\{network}\{f}".format(datalake_path = datalake_path, network=network,f=f),
        sep=";") for f in os.listdir(r"{datalake_path}\networks\{network}".format(datalake_path = datalake_path, network=network)) if "nodes" in f])
    nodes['geometry'] = nodes['geometry'].apply(lambda x: loads(x))
    nodes = gpd.GeoDataFrame(nodes, geometry = 'geometry', crs=25830)
    nodes = nodes.to_crs(4326)

    round_nodes = gpd.GeoDataFrame(
        nodes,
        geometry = nodes.apply(lambda x: Point((round(x['geometry'].x,round_to),round(x['geometry'].y,round_to))),axis=1),
        crs = 4326)
    
    coords = tuple(round_nodes.apply(lambda row: (row['geometry'].x , row['geometry'].y),axis=1).unique())

    return coords

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def get_isochrones(data, coords):
    
    client = ors.Client(key='5b3ce3597851110001cf6248e310e50239484390adc4935f51817f90')
    processed_coords = tuple(set(list(itertools.chain.from_iterable([list(v.keys()) for v in data.values()]))))

    if date.today() in data.keys():
        limit = 500 - len(data[date.today()])
        logger.info(f'API limit today is  {limit}...')
    else: 
        limit = 500
        logger.info(f'API limit today is  {limit}...')
        data[date.today()] = {}

    coords = tuple(c for c in coords if not c in processed_coords)

    logger.info(f'Number of processed coordinates in current execution is {len(processed_coords)} out of {len(coords)}...')

    assert limit > 0, "Warning, you ran out of API calls today. Try again tomorrow"
    
    coords = coords[0:limit]

    for chunk in tqdm(chunker(coords,5)):
        
        isochrone = ors.isochrones.isochrones(
            client,
            chunk,
            profile='driving-car',
            range_type='time',
            range=[30],
            units="m",
            location_type=None,
            smoothing=0.2,
            attributes=None,
            validate=True,
            dry_run=None)['features']

        for i,feature in enumerate(isochrone):
            #print(f"Processing feature {feature}")
            
            if feature['geometry']['type'] == 'Polygon':
                data[date.today()].update({chunk[i] : Polygon(feature['geometry']['coordinates'][0])})
    return data

def dump_data(path, network, data):
    processed_data = json.dumps({date.strftime('%d/%m/%Y'):{f"({k[0]},{k[1]})":v.wkt for k,v in iso.items()} for date, iso in data.items()})
    with open(r"{path}\level0\level0_{network}.json".format(path = path, network=network), 'w') as f:
        json.dump(processed_data, f)


def download_isochrones(path, network):
    processed = storage_file(path, network)
    coords = make_measure_points(network, DATALAKE_PATH, round_to = 2)
    today_processed = get_isochrones(processed, coords)
    dump_data(path, network, today_processed)

def process_isochrones(path, network):
    with open(r"{path}\level0\level0_{network}.json".format(path = path, network=network), 'r') as f:
        
        d = eval(json.load(f))
        #print(d['14/10/2024'])
        flat = {}
        d = {dat:{eval(k): loads(v) for k,v in data.items()} for dat,data in d.items()}
        for dat,data in d.items():
            for k,v in data.items(): flat.update({k:v})
        d = pd.DataFrame.from_dict(flat,orient='index').rename( columns = {0:'geometry'})
        
        d=d.reset_index().rename(columns = {'index':'gridpoint'})
        d['x'] = d.gridpoint.str[0]
        d['y'] = d.gridpoint.str[1]
        
        return d

import numpy as np


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
        return None
        #return pd.Series()


def make_level1(df):
    
    df = gpd.GeoDataFrame(df, geometry='geometry',crs=4326)
    
    df['id'] = "iso_"+df.index.astype(str)
    df['id_class'] = "aois"
    df['category'] = "isochrone"
    df['provider'] = 8
    df.set_index('id',drop=False,inplace=True)
    df['data'] = json_data(df,['x','y'],'id')
    df = df[['id', 'id_class', 'category', 'provider', 'data','geometry']]
    return df
    
    
        

def gather(source_instance, **kwargs):
    download_isochrones(path=kwargs.get('path'), network= kwargs.get('network'))
def level0(source_instance, **kwargs):
    logger.warning('level0 is not needed in this source')
def level1(source_instance, **kwargs):
    df = make_level1(process_isochrones(path=kwargs.get('path'), network= kwargs.get('network')))
    out_data = r"{path}\level2\level2_ors.csv".format(path=kwargs.get('path'))
    logger.info(f'\nSaving LEVEL2 data for ORS...')
    df.to_csv(out_data,sep=";",index=False)
    
