#IMPORTS

import pandas as pd
import re 
from loguru import logger

from sources.metadata.edm2018_metadata import *  

def read_data(path: str, files: dict, schemas: dict):
    return {n:pd.read_excel(
        r"{path}\level0\trips\{p}.xlsx".format(path=path,p=p), sheet_name = n.upper(),dtype=schemas[n]
        ) for n,p in files.items()}
    
def read_codes(path: str, files: dict):
    return {n:pd.read_excel(
    r"{path}\level0\trips\{p}.xlsx".format(path=path,p=p), sheet_name = 'LIBRO DE CODIGOS', header = 0, usecols = ['VARIABLE','VALORES'],skiprows=[1,2], engine ='openpyxl'
    ) for n,p in files.items()}
    
def fill_in_order(df: pd.DataFrame):
    fill = ''
    for i,r in df.iterrows():
        if not pd.isnull(r['VARIABLE']):
            fill = r['VARIABLE']
        else:
            df.at[i,'VARIABLE'] = fill
    return df

def parse_codes(df: pd.DataFrame):
    df['CODIGO'] = df['VALORES'].str.extract(r"""(?m)^(\d+).*""").astype(int)
    df['LITERAL'] = df['VALORES'].str.split(r"""^[^A-Z]*""").str[1].str.strip().str.removesuffix(r"""'""").str.strip()
    return df[['VARIABLE','CODIGO','LITERAL']]

def map_columns(df: pd.DataFrame, mapper: dict):
    
    for c in [x for x in df.columns if x in mapper.keys()]:
        df[c] = df[c].replace(mapper[c])
    return df

def process_edm_data(path: str):
    
    data = read_data(path, FILES, SCHEMAS)
    codes = read_codes(path, FILES)
    codes = {k:fill_in_order(df.query('VALORES.notnull()', engine='python')) for k,df in codes.items()}
    codes = {k:parse_codes(df) for k,df in codes.items()}
    codes = {n: {k : g.LITERAL.to_dict() for k, g in df.set_index('CODIGO').groupby('VARIABLE')} for n,df in codes.items()}
    data = {k:map_columns(df,codes[k]) for k,df in data.items()}

    """
    final = pd.merge(data['etapas'],data['viajes'], on=['ID_HOGAR','ID_IND','ID_VIAJE'], how='left', suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]
    
    final = pd.merge(data['individuos'], final, on=['ID_HOGAR','ID_IND'], how='right',suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]
    
    final = pd.merge(data['hogares'],final, on=['ID_HOGAR'], how='left',suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]
    """

    individuos = pd.merge(data['individuos'],data['hogares'], on=['ID_HOGAR'], how='left', suffixes=("","_x"))
    individuos = individuos[[c for c in individuos.columns if not c.endswith("_x")]]
    
    viajes = pd.merge(data['etapas'], data['viajes'], on=['ID_HOGAR','ID_IND','ID_VIAJE'], how='right',suffixes=("","_x"))
    viajes = viajes[[c for c in viajes.columns if not c.endswith("_x")]]
        
    final = pd.merge(individuos,viajes, on=['ID_HOGAR','ID_IND'], how='left',suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]

    #print(final)
    
    for c in final.columns:
        if final[c].dtype == str:
            final[c] = final[c].fillna("")
        else:
            final[c] = final[c].fillna(0)
    
    final.set_index(['ID_HOGAR','ID_IND','ID_VIAJE','ID_ETAPA'],inplace=True)
    
    #print(final['DNOVIAJO'].unique())

    final.to_csv(r"""{path}\level1\level1_edm2018.csv""".format(path=path))


def gather(source_instance, **kwargs):
    logger.info("EDM does not have a gather mode implemented. Jus check the website and download the excel and shapefile files")
def level0(source_instance, **kwargs):
    process_edm_data(path = kwargs.get('path'))
#def level1(path: str):
    #transform_edm_data(path, codes)
    logger.info("This transformation is not uploaded to the database just yet")
#def persist():
    