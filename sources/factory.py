
# TODO from database.utils import DBInterface REFERENCE IN WALKNET
import pandas as pd
import geopandas as gpd

from os import listdir, remove, makedirs, path, walk
from os.path import exists, join, isfile
import pandas as pd
import geopandas as gpd
import numpy as np
from loguru import logger

class DataTransformer:
    def __init__(self, input_dir, output_dir, target_file_size_mb, source):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.target_file_size_mb = target_file_size_mb
        self.source = source
        
    def transform(self, transform_func,source):
        logger.info("Transformations initiated...")
        temp_file_prefix = f"level2_{source}_"
        file_counter = 1
        current_size = 0
        
        for file_name in listdir(self.input_dir):
            logger.info(f"Opening file {file_name}...")
            if file_name.endswith('.parquet'): #TODO MAKE THIS EXPLICIT FILE TIPE
                input_file = path.join(self.input_dir, file_name)
                gdf = pd.read_parquet(input_file)
                logger.info(f"Processing {file_name} open...")
                
                # Perform batch processing and save transformed batches
                rows_per_batch = int((self.target_file_size_mb * 1024 * 1024) / gdf.memory_usage(deep=True).sum())
                for i, batch_df in enumerate(gdf.groupby(gdf.index // rows_per_batch)):
                    transformed_data = transform_func(batch_df[1])  # Apply the provided transformation function
                    print(i)
                    # Check if the batch should be appended to the previous batch or start a new file
                    if current_size + len(transformed_data) <= self.target_file_size_mb:
                        # Append the batch to the previous file
                        output_file = path.join(self.output_dir, f'{temp_file_prefix}{file_counter}.csv')
                        logger.info(f"Saving file {output_file}...")
                        transformed_data.to_csv(output_file, mode='a', header=False, index=False)
                        current_size += len(transformed_data)
                    else:
                        # Start a new file
                        file_counter += 1
                        output_file = path.join(self.output_dir, f'{temp_file_prefix}{file_counter}.csv')
                        logger.info(f"Saving file {output_file}...")
                        transformed_data.to_csv(output_file, index=False)
                        current_size = len(transformed_data)

class Source():
    from configparser import ConfigParser
    global cfg
    cfg = ConfigParser()
    cfg.read(r'./config.ini')
        
    def __init__(self, keyname=None, provider=None, table=None):
        self.keyname = keyname
        self.provider = provider
        self.table = table
        self.data = f"{cfg.get('DATALAKE','path')}\\sources\\{keyname}"
    
    def run(self, level, **kwargs):
        from importlib import import_module
        s = import_module(f"sources.{self.keyname}",['gather','level0','level1','level2','persist'])            
        assert level in ['gather', 'level0', 'level1', 'level2', 'persist'], "specify a correct level to process"
        if level == 'gather': s.gather(**kwargs)
        elif level == 'level0': s.level0(**kwargs)
        elif level == 'level1': s.level1(**kwargs)
        elif level == 'persist': DBInterface(self.table,self.data,**kwargs).safe_upload()
            
    def inspect_files(startpath):
        #TODO AQUI HACER UNA RECURSION
        for root, dirs, files in walk(startpath):
            level = root.replace(startpath, '').count(sep)
            indent = ' ' * 4 * (level) + '└──'
            print('{}{}/'.format(indent, path.basename(root)))
            subindent = ' ' * 4 * (level + 1)
            if files: print("{}{} files".format(subindent, len(files)))
                #print('{}{}'.format(subindent, f))



            
#TODO Implement __dunder__ stuff into functions

#TODO See if these profiling stuff can improve loggers
"""
import cProfile, pstats, io
from pstats import SortKey
pr = cProfile.Profile()
pr.enable()

df = pd.DataFrame(
    pr.getstats(),
    columns=['func', 'ncalls', 'ccalls', 'tottime', 'cumtime', 'callers'])df = df[df['cumtime']>0.005].sort_values(by='cumtime')
tags = df[df['cumtime']>0.5].set_index('func')['cumtime'].to_dict()
import matplotlib.pyplot as plt
fig,ax= plt.subplots(figsize = (25, 10))
df.plot.bar(ax = ax, x='cumtime',y='tottime')
for text,xy in tags:
    plt.annotate(text,xy)
plt.show()
"""
#TODO Better timers
"""
import time

def timed(func):
    "This decorator prints the execution time for the decorated function."

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.debug("{} ran in {}s".format(func.__name__, round(end - start, 2)))
        return result

    return wrapper
"""