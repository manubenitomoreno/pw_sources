import pandas as pd
from urllib.error import HTTPError
from loguru import logger
from typing import List, Dict

from sources.metadata.ine_adrh_metadata import *

def download_adrh(path: str, tables: List[str]) -> None:
    """
    Downloads Atlas de Renta data from INE for each key in the map.
    
    Parameters:
    path (str): The base path where the files will be saved.

    Returns:
    None
    """
    for key, values in map.items():
        if key in tables:
            url = f"https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/{key}.csv?nocab=1"
            try:
                data = pd.read_csv(url, sep=";")
                data.to_csv(f"{path}\\level0\\{key}_{values[0]}.csv", sep=";", index=False)
                logger.info(f"Data for key {key} successfully downloaded and saved.")
            except HTTPError as err:
                logger.error(f"Failed to download data from url {url}. Error: {err}")
        else: pass
  
def process_adrh(path: str, tables: List[str]) -> None:
    """
    Processes the downloaded Atlas de Renta data.

    :param path: The base path where the files were saved.
    """
    dfs = []
    for key, fields in map.items():
        if key in tables:
            logger.info(f"Reading file:\n{path}\level0\{key}.csv")
            df = pd.read_csv(f"{path}\level0\{key}.csv", sep =";")
            logger.info(f"Parsing geo-codes and periods...")
            df = df[~df['Secciones'].isna()]
            df['code'] = df['Secciones'].str.split(" ").str[0]
            df['name'] = df['Secciones'].str.split(" ", n=1).str[1]
            df['period'] = df['Periodo']
            logger.info(f"Parsing variables...")
            if isinstance(fields,list):
                df['variable'] = df[fields].agg(' - '.join, axis=1)
            else:
                df['variable'] = df[fields]
            df['value'] = df['Total']
            logger.info(f"Making final dataframe...")
            dfs.append(df[['code','name','period','variable','value']])
        else: pass
    pd.concat(dfs).to_parquet(path=f"{path}\level1\ine_adrh.parquet")
    
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
        d = pd.Series(data.set_index(idx)[[c for c in columns if c in data.columns]].to_dict(orient='records'))
        d = d.astype(str).str.replace("'",'"')
        logger.info('Making JSON DATA columns...')
        return d
    except Exception as e:
        logger.error(f"Failed to create JSON data. Error: {e}")
        return pd.Series()
    
def transform_adrh(path: str, codes: List[str], years: List[str]) -> None:
    """
    Transforms the processed Atlas de Renta data. Filters data based on specified codes and years.

    :param path: The base path where the processed files were saved.
    :param codes: List of codes to filter the data.
    :param years: List of years to filter the data.
    """
    in_data = f"{path}\\level1\\level1_ine_adrh.parquet"
    logger.info(f"Reading file:\n{in_data}")
    df = pd.read_parquet(in_data)
    logger.info(f"Filtering values...")
    weird_values = list(df[~df['value'].fillna('-999').str.contains("[0-9]\.[0-9]|[0-9]\,[0-9]|^[0-9]+$", regex=True)]['value'].unique())
    df.loc[df['value'].isin(weird_values), 'value'] = '-999'
    df['value'] = df['value'].fillna('-999').str.replace(".", "").str.replace(",", ".").astype(float)
    # Filter the data by codes and years
    df = df[df['code'].str.startswith(tuple(codes)) & df['period'].astype(str).str.startswith(tuple(years))]
    df = df[df['variable'].isin(variables)]
    logger.info(f"Pivot table and build format...")
    pivot = df.pivot_table(values='value',index=['code','period'],columns='variable')
    pivot = pivot.rename(columns = variables_map)[variables_map.values()].reset_index()
    pivot['data'] = json_data(pivot, variables_map.values(),'period')
    pivot['id'] = pivot['code']+"-"+pivot['period'].astype(str)
    pivot['id_class'] = 'boundaries_data'
    pivot['category'] = 'sociodemographic'
    pivot['provider'] = 3
    pivot['geo_id'] = pivot['code'].astype(str)
    pivot = pivot[['id', 'geo_id', 'id_class', 'category', 'provider', 'data']]
    for code in codes:
        logger.info(f"Saving files to:\n{path}\\level2\\level2_ineadrh_{code}.csv")
        pivot[pivot['id'].str.startswith(code)].to_csv(f"{path}\\level2\\level2_ineadrh_{code}.csv", sep=";", index=False)

def gather(source_instance, **kwargs):
    download_adrh(path = kwargs.get('path'),tables = kwargs.get('tables'))
def level0(source_instance, **kwargs):
    process_adrh(path = kwargs.get('path'), tables = kwargs.get('tables'))
def level1(source_instance, **kwargs):
    transform_adrh(path = kwargs.get('path'), years = kwargs.get('years'), codes = kwargs.get('codes'))