import os
import yaml
from configparser import ConfigParser
from loguru import logger
from typing import Optional
from db_models import DBManager
from networks.make_network import make_network

import geopandas as gpd

class Network:
    cfg = ConfigParser()
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg.read(os.path.join(parent_dir, 'config.ini'))

    def __init__(self, keyname: Optional[str] = None):
        self.keyname = keyname
        self.path = os.path.join(Network.cfg.get('DATALAKE', 'path'), 'networks', keyname)
        self.metadata = {}
        self.load_metadata()
        self.db = DBManager()
        self.data = {}
        self.nodes = gpd.GeoDataFrame()
        self.edges = gpd.GeoDataFrame()
        self.relations = gpd.GeoDataFrame()
        self.make_dataset()
        self.final_split_network = None
        self.nearest = None
        self.final_network_points = None

    @classmethod
    def load_metadata(cls):
        try:
            with open("networks.yaml", "r", encoding="utf-8") as yaml_file:
                networks_metadata = yaml.safe_load(yaml_file)

            if cls.keyname in networks_metadata:
                cls.metadata = networks_metadata[cls.keyname]
            else:
                logger.warning(f"Metadata for '{cls.keyname}' not found in networks.yaml.")
        except FileNotFoundError:
            logger.warning("networks.yaml file not found.")
            
    def make_dataset(self):
        """
        Check if the relevant tables exist in the database for the network. 
        If not, create them.
        """
        tables_to_check = [
            self.metadata.get('road_segments_table'),
            self.metadata.get('pois_table'),
            self.metadata.get('aois_table')
        ]  # you can add more if necessary
        
        # Iterate through the tables and check if they exist
        for table in tables_to_check:
            if not self.db.table_exists(table):
                # If the table doesn't exist, create it
                table_class = self.db.get_table_class(table)
                # Create the table
                table_class.metadata.create_all(self.db.engine)
                logger.info(f"Table '{table}' created in the database.")
            else:
                logger.info(f"Table '{table}' already exists in the database.")


    def call_data(self):
        """
        Retrieve data from the database based on the YAML configuration.
        """
        
        # Querying and storing road_segments
        road_segments_query = self.metadata.get('road_segments_query')
        if road_segments_query:
            self.data['road_segments'] = self.db.get_query_results(road_segments_query)
        else:
            road_segments_table = self.metadata.get('road_segments_table')
            if road_segments_table:
                self.data['road_segments'] = self.db.retrieve_data(road_segments_table)
            else:
                logger.warning("No road_segments query or table provided in YAML.")
        
        # Querying and storing pois
        pois_query = self.metadata.get('pois_query')
        if pois_query:
            self.data['pois'] = self.db.get_query_results(pois_query)
        else:
            pois_table = self.metadata.get('pois_table')
            if pois_table:
                self.data['pois'] = self.db.retrieve_data(pois_table)
            else:
                logger.warning("No pois query or table provided in YAML.")
        
        # Querying and storing aois
        aois_query = self.metadata.get('aois_query')
        if aois_query:
            self.data['aois'] = self.db.get_query_results(aois_query)
        else:
            aois_table = self.metadata.get('aois_table')
            if aois_table:
                self.data['aois'] = self.db.retrieve_data(aois_table)
            else:
                logger.warning("No aois query or table provided in YAML.")
        
        # Querying and storing boundaries_geo
        extent_table = self.metadata.get('extent_table')
        extent_filter = self.metadata.get('extent_filter')
        if extent_table and extent_filter:
            # Assuming that 'extent_filter' is some SQL condition string
            query = f"SELECT * FROM {extent_table} WHERE {extent_filter}"
            self.data['boundaries_geo'] = self.db.get_query_results(query)
        elif extent_table:
            self.data['boundaries_geo'] = self.db.retrieve_data(extent_table)
        else:
            logger.warning("No extent_table provided in YAML.")

    def process_network(self, chunk_size=100):
            """
            Process the road network by splitting it and associating points of interest.
            
            Parameters:
            - chunk_size (int): The distance at which the road network will be split. Defaults to 100 meters.
            
            Returns:
            None. It updates the instance attributes: final_split_network, nearest, final_network_points.
            """
            self.final_split_network, self.nearest, self.final_network_points = make_network(self.road_network, self.points_of_interest, chunk_size)
        

    def upload(self, table, data):
        pass
        # Implement functionality to upload data
        
    def construct(self, **attributes):
        pass
        # Define the sequence in which the methods will be called to construct the network.
        # Example:
        # self.call_data()
        # self.make_network()
        # self.upload(table="some_table", data=self.nodes)

    def run(self, action: str, **kwargs) -> None:
        assert action in ['construct'], "Specify a correct action for the network"
        
        logger.info(f"Calling network: {self.keyname}, Action: {action}")
        attributes = {**self.metadata, **kwargs, 'path': self.path}
        if action == 'construct':
            logger.info("Attempting to construct the network...")
            self.construct(**attributes)
