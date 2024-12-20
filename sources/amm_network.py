import geopandas as gpd
from typing import List

def transform_amm(path: str, codes: List[str]):
    id_range = 0
    for code in codes:
        gdf = gpd.read_file(path+f"\level1\level1_amm_network_{code}.gpkg", layer =f'level1_amm_network_{code}')
        if 'id' in gdf.columns: gdf.drop(columns=['id'], inplace=True)
        gdf.insert(id_range, 'id', range(id_range, id_range + len(gdf)))
        id_range = id_range + len(gdf)
        print(id_range)
        gdf['id_class'] = 'road_segments'
        gdf['category'] = 'pedestrian'
        gdf['provider'] = gdf['networkGrp']
        gdf = gdf.explode(ignore_index=True)
        gdf['length'] = gdf.apply(lambda x: round(x['geometry'].length,2), axis = 1)
        gdf['data'] = "{'length': '" +gdf['length'].astype(str)+"'}"
        gdf = gdf[['id','id_class','category','provider','data','geometry']]

        gdf.to_csv(path+f"\level2\level2_amm_network_{code}.csv",sep=';',index=False)

#TODO Fix this into proper level0 and level1
def level1(source_instance, **kwargs):
    transform_amm(path=kwargs.get('path'), codes= kwargs.get('codes'))
#def level0(path: str, codes:list):
    #process_cadastral_data(path, codes)
    #logging.INFO('Processing level0 for...')
#def level1(path: str):
    #process_edm_data(path, files, schemas)
    #logging.INFO('Processing level1 for...')
#def persist():
    #logging.INFO('Persisting data for...')