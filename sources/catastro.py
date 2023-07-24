import requests, zipfile, io, subprocess
from os import listdir, remove, makedirs, environ, remove
from os.path import exists, join
import pandas as pd
import geopandas as gpd
import numpy as np
from loguru import logger

from sources.metadata.catastro_metadata import *

#GATHER FUNCTIONALITY


def find_muni(code: str) -> tuple:
    """
    Searches for municipality code in LISTMUNI. Make sure this list is up to date.
    
    Parameters:
    code (str): A string with municipality code 

    Returns:
    tuple: A tuple with a boolean indicating if the municipality was found and the municipality itself
    """
    found = [m for m in LISTMUNI if m.split("-")[0] == code][0]
    if found:
        logger.info(f"Found {code} in API reference")
        return True, found
    else:
        logger.error(f"Check code {code}. Code not found.")
        return False, None

def gml2geojson(input: str, output: str) -> None:
    """
    Convert a GML to a GeoJSON file.
    
    Parameters:
    input (str): Input GML file path
    output (str): Output GeoJSON file path

    Returns:
    None
    """
    environ['PROJ_LIB'] = 'C:\\Users\\ManuBenito\\Documents\\GitHub\\pw_sources\\venv\\lib\\site-packages\\pyproj\\proj_dir\\share\\proj'
    try:
        # If output file already exists, delete it
        if exists(output):
            logger.info(f"GEOJson already exist for {output}. Deleting")
            remove(output)
            
        connect_command = f"ogr2ogr -f GeoJSON {output} {input} -a_srs EPSG:25830 -skipfailures"
        process = subprocess.Popen(connect_command, shell=True)
        process.communicate()
        process.wait()
        logger.info(f"GML {input} converted to {output}.geojson")
    except Exception as err:
        logger.error(f"Failed to convert to GeoJSON from GML: {err}")
        raise

def download_cadastral_data(codes: list, outdir:str) -> None:
    """
    Download cadastral data for given codes and save in specified directory.
    
    Parameters:
    codes (list): List of municipality codes
    outdir (str): Output directory to save the data

    Returns:
    None
    """
    outdir = outdir+r"\level0\spatial"
    for code in codes:
        assert find_muni(code)[0], f"Municipality with code {code} not found"
        
        muni = find_muni(code)[1]
        
        codine_nomcatastro = [f for f in LISTMUNI if f.startswith(code)][0]
        codine_nomcatastro = "-".join([codine_nomcatastro.split('-', 1)[0],codine_nomcatastro.split('-', 1)[-1].replace("-"," ")])
        codmun = codine_nomcatastro[0:5]
        codprov = codine_nomcatastro[0:2]
        outdir_muni = r"{outdir}\{codmun}".format(outdir=outdir,codmun=codmun)
        if not exists(outdir_muni):
            makedirs(outdir_muni)

        urls = {'CadastralParcels' : f"http://www.catastro.minhap.es/INSPIRE/CadastralParcels/{codprov}/{codine_nomcatastro}/A.ES.SDGC.CP.{codmun}.zip",
                'Addresses' : f"http://www.catastro.minhap.es/INSPIRE/Addresses/{codprov}/{codine_nomcatastro}/A.ES.SDGC.AD.{codmun}.zip",
                'Buildings' : f"http://www.catastro.minhap.es/INSPIRE/Buildings/{codprov}/{codine_nomcatastro}/A.ES.SDGC.BU.{codmun}.zip"}

        for layer, url in urls.items():
            logger.info(f"Getting URL {url}")
            r = requests.get(url)
            if r:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(outdir_muni)
            else:
                logger.warning(f'Municipality {codine_nomcatastro} had no response from ATOM')
                continue
        
        layers = {"AD": "addresses","building": "building","buildingpart": "buildingpart","otherconstruction": "otherconstruction", "cadastralparcel": "cadastralparcel","cadastralzoning": "cadastralzoning"}
        
        for gml in [join(outdir_muni, f) for f in listdir(outdir_muni) if code in f and f.endswith(("gml"))]:
            for particle, layer in layers.items():
                if particle in gml:
                    gml2geojson(gml, outdir_muni+f"\{layer}_{codmun}.geojson")
                    
        for otherfile in [join(outdir_muni, f) for f in listdir(outdir_muni) if code in f and not f.endswith(("geojson"))]:
            remove(otherfile)
        
        assert len([f for f in listdir(outdir_muni) if code in f and f.endswith(("geojson"))]) == len(layers), "Not all files expected were processed"
        
        logger.info(f"Municipality {muni} was successfully downloaded and processed")

#LEVEL0

def find_muni(code: str) -> bool:
    """
    Searches for municipality code in metadata LISTMUNI. Make sure this list is up to date
    :code: A string with municipality code 
    :returns: boolean evaluation if municipality code is found
    """
    found = [m for m in LISTMUNI if m.split("-")[0] == code][0]
    if found:
        return True, found
    else:
        print(f"Check code {code}. Code not found")
        return False

def files_for_muni(code: str, path: str) -> tuple:
    
    path = path+r"\level0"
    if code[2] == "0":
        file_code = "_".join([code[0:2],code[3:]])
    else:
        file_code = "_".join([code[0:2],code[2:]])
        
    cadastre_path = [join(path+r"\non-spatial", f) for f in listdir(path+r"\non-spatial") if file_code in f and f.endswith(".CAT")][0]
    addresses_path = [join(path+r"\spatial\{code}".format(code=code), f) for f in listdir(path+r"\spatial\{code}".format(code=code)) if code in f and "addresses" in f and f.endswith(".geojson")][0]
    parcels_path = [join(path+r"\spatial\{code}".format(code=code), f) for f in listdir(path+r"\spatial\{code}".format(code=code)) if code in f and "cadastralparcel" in f and f.endswith(".geojson")][0]
    
    return cadastre_path, addresses_path, parcels_path

def merge_and_drop(data1: pd.DataFrame, data2: pd.DataFrame, op: str, join_list: list) -> pd.DataFrame:
    """
    Merges DataFrames on a list of columns and drops duplicates on the desired side
    
    :data1: A first Pandas DataFrame
    :data2: A second Pandas DataFrame
    :op: Pandas merge 'how' argument
    :join_list: A list of common columns to join by
    :return: Returns joined DataFrame without duplicated or suffixed columns
    """
    df = pd.merge(data1 , data2 ,how = op, on = join_list , suffixes = ("","_r"))
    df = df[[col for col in df.columns if col[-2:] != "_r"]]
    return df

def open_cat_file(cadastre_path: str) -> pd.DataFrame:
    """
    Opens a .CAT Cadastre file, slices its positions according to the documentation,
    builds four tables (one for each register type) and merges all its information
    :cadastre_path: A valid path to a .CAT file 
    :return: Returns joined DataFrame with all four cadastral property registers merged
    """
    df0 = pd.read_csv(cadastre_path, delimiter="··", header = None, encoding='latin-1', engine='python') #Read .CAT file
    df0.rename(columns={0:'raw'},inplace=True)    
    df0['reg'] = df0['raw'].str.slice(0,2) #This first position defines the kind of table within the file     
    raw_tables = {n:pd.DataFrame(df0[df0['reg']==n]) for n in ['11','13','14','15']} #We are interested in this four
    #Now we slice the tables according to our metadata
    for register,df in raw_tables.items():
        for field,separators in CAT_FILE_REGISTERS[register].items():
            df[field] = df['raw'].str.slice(separators[0],separators[1]).str.strip()
        df.drop(columns = ['raw','reg'], axis = 1, inplace = True)
    #And merge according to their inner logic
    df = merge_and_drop(raw_tables['15'],raw_tables['14'],'right',['Parcel Cadastral ID','Property ID'])
    df = merge_and_drop(raw_tables['13'],df,'right',['Parcel Cadastral ID','Building Code ID'])
    df = merge_and_drop(raw_tables['11'],df,'right',['Parcel Cadastral ID'])
    return df

def column_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies a strategy to each column, as defined by the metadata in string format
    :df: The raw dataframe with cadastral data split in columns
    :return: The processed dataframe
    """
    import numpy as np
    for col in COLUMN_TREATMENT:
        if COLUMN_TREATMENT[col]['strategy']!='None':
            df[col] = eval(COLUMN_TREATMENT[col]['strategy'])
        else:
            pass
    return df
        
def make_address_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Makes a column that holds a city_gml normalized ID, in order to join data to INSPIRE addresses
    :df: The joined cadastral registers dataframe
    :return: The dataframe with a normalized gml_id address format
    """
    df['Property gml Number'] = df['Property First Number']+df['Property First Letter']
    address_prefix = 'ES.SDGC.AD.'
    df['Property gml_id'] = df[['Parcel Province Code',
                                'Parcel Municipality Code INE',
                                'Parcel Street Code',
                                'Property gml Number',
                                'Parcel Cadastral ID']].agg('.'.join, axis=1)
    df['Property gml_id'] = address_prefix + df['Property gml_id']
    return df
    
def merge_addresses_points(cadastral_data: pd.DataFrame, addresses_path:str) -> gpd.GeoDataFrame:
    """
    Reads the corresponding addresses file
    :df: The joined cadastral registers dataframe
    :return: The dataframe with a normalized gml_id address format
    """
    addresses_inspire = gpd.read_file(addresses_path)[['gml_id','geometry']]
    addresses_inspire = addresses_inspire.to_crs("EPSG:4326")
    gdf = pd.merge(addresses_inspire, cadastral_data, left_on='gml_id', right_on = 'Property gml_id',how='right')
    consistent_by_address = gdf[~gdf['gml_id'].isna()]
    not_found  = gdf[gdf['gml_id'].isna()][list(cadastral_data.columns)]
    addresses_inspire['gml_id'] = addresses_inspire['gml_id'].str.split(".").str[-1]
    addresses_inspire = addresses_inspire.drop_duplicates(subset=['gml_id'], keep='first')
    gdf = pd.merge(addresses_inspire,not_found,left_on = 'gml_id', right_on = 'Parcel Cadastral ID',how='right')
    consistent_by_parcel = gdf[~gdf['gml_id'].isna()]
    gdf = pd.concat([consistent_by_address,consistent_by_parcel],ignore_index = True)
    return gdf

def detect_multiproperty(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Reads the corresponding addresses file
    :df: The joined cadastral registers dataframe
    :return: The dataframe with a normalized gml_id address format
    """
    relation = gdf.groupby(['Parcel Cadastral ID'])['Property ID'].nunique()
    gdf['Property Single or Multiple'] =''
    gdf.loc[gdf['Parcel Cadastral ID'].isin(list(relation[relation!=1].index)),'Property Single or Multiple'] = 'Multiple Property'
    gdf.loc[gdf['Parcel Cadastral ID'].isin(list(relation[relation==1].index)),'Property Single or Multiple'] = 'Single Property'
    return gdf

def build_use_names(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Reads the corresponding addresses file
    :df: The joined cadastral registers dataframe
    :return: The dataframe with a normalized gml_id address format
    """
    gdf['Property Cadastral ID'] = gdf['Parcel Cadastral ID']+"-"+gdf['Property ID']
    gdf['Part Area in Cadastral Terms'] = gdf['Part Area in Cadastral Terms'].fillna("0").astype(int)
    gdf['Part Typology'] = gdf['Part Typology'].str[:-1].fillna("00")
    gdf['Property Land Use Level1'] = gdf['Property General Use'].replace(LAND_USE)
    gdf['Part Land Use Level1'] = gdf['Part Detailed Use'].str[0].replace(LAND_USE)
    gdf['Part Land Use Level2'] = gdf['Part Detailed Use'].replace(LAND_USE)
    gdf['Part Typology Level1'] = gdf['Part Typology'].str[0:2].replace(TYPOLOGY)
    gdf['Part Typology Level2'] = gdf['Part Typology'].str[0:3].replace(TYPOLOGY)
    gdf['Part Typology Level3'] = gdf['Part Typology'].str[0:4].replace(TYPOLOGY)
    return gdf

def process_cadastral_data(path: str, codes: str) -> gpd.GeoDataFrame:
    """
    Reads the corresponding addresses file
    :df: The joined cadastral registers dataframe
    :return: The dataframe with a normalized gml_id address format
    """
    for code in codes:
        municipality = find_muni(code)[1]
        cadastre_path, addresses_path, parcels_path = files_for_muni(code, path)
        df = open_cat_file(cadastre_path)
        df = column_strategy(df)
        #logging.INFO(f'{municipality} .cat file processed')
        df = make_address_column(df)
        #logging.INFO(f'{municipality} address column prepared')
        gdf = merge_addresses_points(df, addresses_path)
        #logging.INFO(f'{municipality} address spatial data merged')
        gdf = detect_multiproperty(gdf)
        gdf = build_use_names(gdf)
        #logging.INFO(f'{municipality} land use and typology procesed')
        #gdf.drop(columns=['geometry']).to_parquet(path+r"\level1\{municipality}.parquet".format(municipality=municipality))
        gdf['x'] = gdf['geometry'].x
        gdf['y'] = gdf['geometry'].y
        gdf.to_parquet(path+r"\level1\{municipality}.parquet".format(municipality=municipality))
    
#LEVEL1

def apply_logic(df: pd.DataFrame, combination_tuple: tuple, end_type: str, category):
    """
    Inputs a series of combinations of land use and typology columns and builds a conditional that is mapped to a Walknet category, and builds final columns
    :df: The joined cadastral registers dataframe
    :combination_tuple: A tuple for filtering the data
    :endt_type: A string label of the end type
    :category: A string label of walknet category
    :return: The dataframe with a normalized gml_id address format
    """
    CLASSES = ['Property Land Use Level1', 'Part Typology Level1', 'Part Typology Level2', 'Part Typology Level3', 'Part Land Use Level1', 'Part Land Use Level2']
    #logging.info(f'classes columns passed...{CLASSES}')
    #logging.info(f'combinations passed...{combination_tuple}')
    combination_tuple = tuple(['no'] if not isinstance(x,list) else x for x in combination_tuple)
    #logging.info(f'combinations passed...{combination_tuple}')
    conditions = "&".join([f"(df['{field}'].isin(combination_tuple[{i}]))" for i,field in enumerate(CLASSES) if not combination_tuple[i] == ['no']])
    #logging.info(f'conditions are...{conditions}')
    df['End Type'] = end_type
    df.loc[eval(conditions),'Walknet Category'] = end_type+" - "+category
    return df   
      
def group_data(gdf):
    #logger.info(f'grouping area and count statistics by reference point...')

    common = ['Parcel Cadastral ID', 'Property Cadastral ID','x','y','End Type','Walknet Category']
    local = ['Part Area in Cadastral Terms']
    #logger.info(f'pivoting values...')
    t = gdf.groupby(common).sum()[local]
    pv = pd.pivot_table(t.reset_index(),values='Part Area in Cadastral Terms', index = ['Parcel Cadastral ID', 'Property Cadastral ID','x','y'],columns = ['Walknet Category'])
    #logger.info(f'area values...')
    pv = pv.groupby(['x','y']).sum().rename(columns={c:f"{c} - Area" for c in pv.columns})
    cv = t.groupby(common).count()[local]
    #logger.info(f'count values...')
    cv = pd.pivot_table(cv.reset_index(),values='Part Area in Cadastral Terms', index = ['Parcel Cadastral ID', 'Property Cadastral ID','x','y'],columns = ['Walknet Category'])
    cv = cv.groupby(['x','y']).sum().rename(columns={c:f"{c} - Number" for c in cv.columns})
    #logger.info(f'merging values into final table...')
    df = pd.concat([cv,pv],axis=1).reset_index()
    
    return df

def json_data(data, columns, idx):
    d =  pd.Series(data.set_index(idx)[[c for c in columns if c in data.columns]].to_dict(orient='records'))
    d = d.astype(str).str.replace("'",'"')
    #logging.info(f'Making JSON DATA columns...')
    return d

def transform_data(gdf):
    #logging.info(f'Transforming data...')
    CLASSES = ['Property Land Use Level1', 'Part Typology Level1', 'Part Typology Level2', 'Part Typology Level3', 'Part Land Use Level1', 'Part Land Use Level2']
    RELATIONS = pd.read_excel(r"C:\Users\ManuBenito\Documents\GitHub\walknet\sources\spain\landuse\catastro\metadata\relations.xls")
    for i,row in RELATIONS.iterrows():
        combination = tuple([row[col].split(";") if isinstance(row[col],str) else None for col in CLASSES])
        end_type = row['End Type']
        category = row['Walknet Category']
        gdf = apply_logic(gdf,combination,end_type,category)
        
    gdf = group_data(gdf)
    #logging.info(f'Building last columns...')
    gdf['id'] = np.arange(0,len(gdf),1)
    gdf['class'] = 'pois'
    gdf['category'] = 'land use'
    gdf['provider'] = 'Dirección General de Catastro'
    gdf['data'] = json_data(gdf, DATACOLS, 'id' )
    gdf['geometry'] = gpd.GeoSeries(gpd.points_from_xy(gdf['x'], gdf['y'])).to_wkt()
    return gdf[['id','class','category','provider','data','geometry']]

def transform_cadastral_data(path: str, codes: str):
    path = f"{path}\level1"
    for code in codes:
        npath = [join(path, f) for f in listdir(path) if f.startswith(code)][0]
        df = pd.read_parquet(npath)
        df = transform_data(df)
        df.to_csv('{newpath}\level2_catastro_{code}.csv'.format(newpath= path.replace("level1","level2"), code = code),sep =";")
"""
============================================================================
============================================================================
MAIN FUNCTIONS FOR CATASTRO SOURCE - SPAIN
============================================================================
CONTRIBUITORS: MANU BENITO
============================================================================
"""

def gather(source_instance, **kwargs):
    download_cadastral_data(codes = kwargs.get('codes'), outdir=kwargs.get('path'))

def level0(source_instance, **kwargs):
    process_cadastral_data(path=kwargs.get('path'), codes = kwargs.get('codes'))
    
def level1(source_instance, **kwargs):
    transform_cadastral_data(path=kwargs.get('path'), codes = kwargs.get('codes'))


    #transform_cadastral_data(path, codes)
    #logging.INFO('Processing level1 for...')