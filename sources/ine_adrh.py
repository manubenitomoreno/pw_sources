import pandas as pd
from sources.ine_adrh_metadata import *


def download_adrh(path):
    for key, values in map.items():
        url = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/{key}.csv?nocab=1"
        pd.read_csv(url,sep=";").to_csv(f"{path}\level0\{key}_{values[0]}.csv",sep=";")
        
def process_adrh(path):
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
    
def transform_adrh(path):
    
    df = pd.read_parquet(f"{path}\level1\ine_adrh.parquet")

    weird_values = list(df[~df['value'].fillna('-999').str.contains("[0-9]\.[0-9]|[0-9]\,[0-9]|^[0-9]+$",regex=True)]['value'].unique())

    df.loc[df['value'].isin(weird_values),'value'] = '-999'

    df['value'] = df['value'].fillna('-999').str.replace(".","").str.replace(",",".").astype(float)
    
    df[df['variable'].isin(variables)]
    
    return df

def gather(path: str):
    download_adrh(path)
    #logging.DEBUG('Gathering data for...')
def level0(path: str):
    process_adrh(path)
    #logging.INFO('Processing level0 for...')
def level1(path: str):
    transform_adrh(path)
    #logging.INFO('Processing level1 for...')
#def persist():
    #logging.INFO('Persisting data for...')