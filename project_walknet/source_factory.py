import os
import yaml
from configparser import ConfigParser
from importlib import import_module
from typing import Optional
from loguru import logger

from db_models import DBManager

class Source:
    cfg = ConfigParser()
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg.read(os.path.join(parent_dir, 'config.ini'))

    def __init__(self, keyname: Optional[str] = None, **kwargs):
        self.keyname = keyname
        self.path = f"{Source.cfg.get('DATALAKE', 'path')}\\sources\\{keyname}"
        self.metadata = {}
        self.load_metadata()
        self.additional_attributes = kwargs
        self.db = DBManager()

    def load_metadata(self):
        try:
            with open("sources.yaml", "r", encoding="utf-8") as yaml_file:
                sources_metadata = yaml.safe_load(yaml_file)

            if self.keyname in sources_metadata:
                self.metadata = sources_metadata[self.keyname]
            else:
                logger.warning(f"Metadata for '{self.keyname}' not found in sources.yaml.")
        except FileNotFoundError:
            logger.warning("sources.yaml file not found.")
            
    def persist(self, **kwargs):
        # Use self.db to perform DB operations
        print(self.metadata['table'])
        
        if isinstance(self.metadata['table'], dict):
            for suffix, table in self.metadata['table'].items():

                if self.db.table_exists(table, target_schema = 'sources'):
                    
                    # Get the SQLAlchemy class for the given tablepy
                    table_class = self.db.get_table_class(f"{table}")
                    
                    # Path to the level 2 data
                    level2_data_path = os.path.join(self.path, 'level2')
                    
                    # Loop through all the csv files in the directory
                    for filename in os.listdir(level2_data_path):
                        if filename.endswith(f'{suffix}.csv'):
                            
                            # Construct the full file path
                            csv_file_path = os.path.join(level2_data_path, filename)

                            # Add data from the csv file to the database
                            self.db.add_data_from_csv(table_class, csv_file_path)
                else:
                    logger.warning(f"Table {table} not found")

        else: 

            table = self.metadata['table']

            if self.db.table_exists(table, target_schema = 'sources'):
                
                # Get the SQLAlchemy class for the given tablepy
                table_class = self.db.get_table_class(f"{table}")
                
                # Path to the level 2 data
                level2_data_path = os.path.join(self.path, 'level2')
                
                # Loop through all the csv files in the directory
                for filename in os.listdir(level2_data_path):
                    if filename.endswith('.csv'):
                        
                        # Construct the full file path
                        csv_file_path = os.path.join(level2_data_path, filename)

                        # Add data from the csv file to the database
                        self.db.add_data_from_csv(table_class, csv_file_path)
            else:
                logger.warning(f"Table {table} not found")

                    
    def run(self, level: str, **kwargs) -> None:
        s = import_module(f"sources.{self.keyname}", ['gather', 'level0', 'level1', 'persist'])
        assert level in ['gather', 'level0', 'level1', 'persist'], "Specify a correct level to process"
        
        logger.info(f"Calling source: {self.keyname}, Action: {level}")
        
        attributes = {**self.metadata, **kwargs, **self.additional_attributes, 'path': self.path}
        
        
        if level in ['gather', 'level0', 'level1']:
            source_method = getattr(s, level)
            source_method(self, **attributes) 
        elif level == 'persist':
            logger.info("Attempting to upload into DB...")
            self.persist()
                

        
