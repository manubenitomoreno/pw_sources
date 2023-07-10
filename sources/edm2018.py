#IMPORTS

import pandas as pd
import re 

from sources.edm2018_metadata import *  
"""
============================================================================
============================================================================
GATHERER FOR EDM2018 SOURCE - SPAIN
============================================================================
CONTRIBUITORS: MANU BENITO
============================================================================
"""

"""
============================================================================
============================================================================
LEVEL0 FOR EDM2018 SOURCE - SPAIN
============================================================================
CONTRIBUITORS: MANU BENITO
============================================================================
Reads all Excel files (both data and codes sheets)
Consolidates data and mapping codes
Merges data together
"""    
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

def process_edm_data(path: str, files: dict, schemas: dict):
    
    data = read_data(path, files, schemas)
    codes = read_codes(path, files, schemas)
    codes = {k:fill_in_order(df.query('VALORES.notnull()', engine='python')) for k,df in codes.items()}
    codes = {k:parse_codes(df) for k,df in codes.items()}
    codes = {n: {k : g.LITERAL.to_dict() for k, g in df.set_index('CODIGO').groupby('VARIABLE')} for n,df in codes.items()}
    data = {k:map_columns(df,codes[k]) for k,df in data.items()}
    
    final = pd.merge(data['etapas'],data['viajes'], on=['ID_HOGAR','ID_IND','ID_VIAJE'], how='left', suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]
    
    final = pd.merge(data['individuos'], final, on=['ID_HOGAR','ID_IND'], how='right',suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]
    
    final = pd.merge(data['hogares'],final, on=['ID_HOGAR'], how='left',suffixes=("","_x"))
    final = final[[c for c in final.columns if not c.endswith("_x")]]
    
    for c in final.columns:
        if final[c].dtype == str:
            final[c] = final[c].fillna("")
        else:
            final[c] = final[c].fillna(0)
    
    final.set_index(['ID_HOGAR','ID_IND','ID_VIAJE'],inplace=True)
    
    final.to_parquet(r"""{path}\level1\level1_edm2018.parquet""".format(path=path))

"""
============================================================================
============================================================================
MAIN FUNCTIONS FOR EDM2018 SOURCE - SPAIN
============================================================================
CONTRIBUITORS: MANU BENITO
============================================================================
"""

#def gather(path: str, codes:list):
    #download_cadastral_data(codes, path)
    #logging.DEBUG('Gathering data for...')
#def level0(path: str, codes:list):
    #process_cadastral_data(path, codes)
    #logging.INFO('Processing level0 for...')
def level1(path: str):
    process_edm_data(path, files, schemas)
    #logging.INFO('Processing level1 for...')
#def persist():
    #logging.INFO('Persisting data for...')