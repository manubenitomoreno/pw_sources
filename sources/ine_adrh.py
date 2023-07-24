import pandas as pd
from urllib.error import HTTPError
from loguru import logger
from typing import List, Dict


from sources.metadata.ine_adrh_metadata import *

def download_adrh(path: str) -> None:
    """
    Downloads Atlas de Renta data from INE for each key in the map.
    
    Parameters:
    path (str): The base path where the files will be saved.

    Returns:
    None
    """
    for key, values in map.items():
        url = f"https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/{key}.csv?nocab=1"
        try:
            data = pd.read_csv(url, sep=";")
            data.to_csv(f"{path}\\level0\\{key}_{values[0]}.csv", sep=";", index=False)
            logger.info(f"Data for key {key} successfully downloaded and saved.")
        except HTTPError as err:
            logger.error(f"Failed to download data from url {url}. Error: {err}")

        
def process_adrh(path: str) -> None:
    """
    Processes the downloaded Atlas de Renta data.

    :param path: The base path where the files were saved.
    """
    dfs = []
    for key, fields in map.items():
        df = pd.read_csv(f"{path}\level0\{key}.csv", sep =";")
        df = df[~df['Secciones'].isna()]
        df['code'] = df['Secciones'].str.split(" ").str[0]
        df['name'] = df['Secciones'].str.split(" ", n=1).str[1]
        df['period'] = df['Periodo']
        if isinstance(fields,list):
            df['variable'] = df[fields].agg(' - '.join, axis=1)
        else:
            df['variable'] = df[fields]
        df['value'] = df['Total']
        dfs.append(df[['code','name','period','variable','value']])
    pd.concat(dfs).to_parquet(path=f"{path}\level1\ine_adrh.parquet")

def transform_adrh(path: str, codes: List[str], years: List[str]) -> None:
    """
    Transforms the processed Atlas de Renta data. Filters data based on specified codes and years.

    :param path: The base path where the processed files were saved.
    :param codes: List of codes to filter the data.
    :param years: List of years to filter the data.
    """
    df = pd.read_parquet(f"{path}\\level1\\level1_ine_adrh.parquet")
    weird_values = list(df[~df['value'].fillna('-999').str.contains("[0-9]\.[0-9]|[0-9]\,[0-9]|^[0-9]+$", regex=True)]['value'].unique())
    df.loc[df['value'].isin(weird_values), 'value'] = '-999'
    df['value'] = df['value'].fillna('-999').str.replace(".", "").str.replace(",", ".").astype(float)
    
    # Filter the data by codes and years
    df = df[df['code'].str.startswith(tuple(codes)) & df['period'].astype(str).str.startswith(tuple(years))]

    df = df[df['variable'].isin(variables)]
    #logger.info(df.head())
    for code in codes:
        df[df['code'].str.startswith(code)].to_csv(f"{path}\\level2\\level2_ineadrh_{code}.csv", sep=";", index=False)

def gather(source_instance, **kwargs):
    download_adrh(path = kwargs.get('path'))
def level0(source_instance, **kwargs):
    process_adrh(path = kwargs.get('path'))
def level1(source_instance, **kwargs):
    transform_adrh(path = kwargs.get('path'), years = kwargs.get('years'), codes = kwargs.get('codes'))
#def persist():
    #logging.INFO('Persisting data for...')