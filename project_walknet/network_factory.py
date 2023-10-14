import os
import yaml
from configparser import ConfigParser
from loguru import logger
from typing import Optional
from db_models import DBManager
from sqlalchemy import text
import pandas as pd
from networks.make_network import make_network
from shapely import wkt
import geopandas as gpd

#TODO DISABLE THIS
import warnings
warnings.filterwarnings("ignore")

class Network:
    cfg = ConfigParser()
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg.read(os.path.join(parent_dir, 'config.ini'))

    def __init__(self, keyname: Optional[str] = None, ):
        self.keyname = keyname
        self.path = os.path.join(Network.cfg.get('DATALAKE', 'path'), 'networks', keyname)
        self.metadata = {}
        self.load_metadata()
        self.db = DBManager()
        self.data = {}
        self.nodes = gpd.GeoDataFrame()
        self.edges = gpd.GeoDataFrame()
        self.relations = gpd.GeoDataFrame()
        
        """
        YOU ARE HERE
        la idea es construct (and persist)
        despues load (red cargada)
        y despues metrics definidas en el yaml
        make partition attribute (in this case, codes)
            check if tables and queries exist
            make schema if it does not exist
            do retrieve data from the db
            transform data retrieved
            make final db tables
            persist those tables
            retrieve data from db and build Network
            get only relevant template generator
            calculate shortest paths and cfbc and ego_graph
            persist those paths
        #self.final_split_network = None
        #self.nearest = None
        #self.final_network_points = None
        """

    #@classmethod
    def load_metadata(self):
        try:
            with open("networks.yaml", "r", encoding="utf-8") as yaml_file:
                networks_metadata = yaml.safe_load(yaml_file)
                if self.keyname in networks_metadata.keys():
                    self.metadata = networks_metadata[self.keyname]
                else:
                    logger.warning(f"Metadata for '{self.keyname}' not found in networks.yaml.")
                    
        except FileNotFoundError:
            logger.warning("networks.yaml file not found.")
            
   
    def check_tables(self):
        """
        Check whether source tables and network tables exist.
        Returns:
            (bool, bool): A tuple. The first bool indicates if source tables exist. The second bool indicates if network tables exist.
        """
        sources_exist = all(self.db.table_exists(table, 'sources') for table in [
            self.metadata.get('road_segments_table'),
            self.metadata.get('pois_table'),
            self.metadata.get('extent_table')
        ])

        network_tables = [f"{self.keyname}_nodes", f"{self.keyname}_edges", f"{self.keyname}_relations"]
        networks_exist = all(self.db.table_exists(table, self.keyname) for table in network_tables)
        
        return sources_exist, networks_exist
                
    def make_tables(self):
        table_names = [
        "nodes",
        "edges",
        "relations"]
        self.db.create_all(table_names, 'networks', self.keyname)#, schema = 'networks') 
    
    def call_data(self):
        """
        Retrieve data from the database based on the YAML configuration.
        """
        
        # Querying and storing road_segments
        road_segments_table = self.metadata.get('road_segments_table')
        pois_table = self.metadata.get('pois_table')
        
        road_segments_where = self.metadata.get('road_segments_query')
        pois_where = self.metadata.get('pois_query')
        
        extent_table = self.metadata.get('extent_table')
        extent_filter = self.metadata.get('extent_filter')
        
        road_segments_fields = "s.id,s.id_class,s.category,s.provider,s.data, st_asewkt(s.geometry) geometry"
        pois_fields = "s.id,s.id_class,s.category,s.provider,s.data, st_asewkt(s.geometry) geometry"
         
        road_segments_query = f"SELECT {road_segments_fields} FROM sources.{road_segments_table} s WHERE {road_segments_where}"
        pois_query = f"SELECT * FROM {pois_table} s WHERE {pois_where} AND {extent_filter}"
        
        spatial_join = """ST_INTERSECTS(ST_Transform(st_setsrid(st_geomfromewkt(st_asewkt(s.geometry)),25830), 4326),st_setsrid(st_geomfromewkt(st_asewkt(e.geometry)),4326))"""
        
        if road_segments_query:
            logger.info("Querying for road segments data")
            if extent_table:
                road_segments_query = f"SELECT {road_segments_fields} FROM sources.{road_segments_table} s, sources.{extent_table} e WHERE {road_segments_where} AND {spatial_join} AND e.{extent_filter}"
                self.data['road_segments'] = pd.DataFrame.from_dict(self.db.get_query_results(text(road_segments_query)))
                self.data['road_segments']['geometry'] = self.data['road_segments']['geometry'].apply(wkt.loads)
                self.data['road_segments'] = gpd.GeoDataFrame(
                    self.data['road_segments'],
                    geometry='geometry',
                    crs = "EPSG:25830")

            else:
                #TODO APPLY WKT BEFORE INSTANTIATING GPD
                self.data['road_segments'] = gpd.GeoDataFrame(
                    pd.DataFrame.from_dict(
                        self.db.get_query_results(text(road_segments_query))),
                    geometry='geometry',
                    crs = "EPSG:25830")
                
            logger.info("Road segments data ready")
            
        if pois_query:
            logger.info("Querying for pois data")
            if extent_table:
                
                pois_query = f"SELECT {pois_fields} FROM sources.{pois_table} s, sources.{extent_table} e WHERE {pois_where} AND {spatial_join} AND e.{extent_filter}"
                                
                self.data['pois'] = pd.DataFrame.from_dict(
                        self.db.get_query_results(text(pois_query)))
                self.data['pois']['geometry'] = self.data['pois']['geometry'].apply(wkt.loads)
                self.data['pois'] = gpd.GeoDataFrame(self.data['pois'],
                    geometry='geometry',
                    crs = "EPSG:25830")
                                
            else:
                #TODO APPLY WKT BEFORE INSTANTIATING GPD
                self.data['pois'] = gpd.GeoDataFrame(
                    pd.DataFrame.from_dict(
                        self.db.get_query_results(text(pois_query))),
                    geometry='geometry',
                    crs = "EPSG:25830")
            logger.info("POIs data ready")
            
    def process_network(self, chunk_size=100):
        """
        Process the road network by splitting it and associating points of interest.
        
        Parameters:
        - chunk_size (int): The distance at which the road network will be split. Defaults to 100 meters.
        
        Returns:
        None. It updates the instance attributes: final_split_network, nearest, final_network_points.
        """
        final_split_network, nearest, final_network_points = make_network(self.data['road_segments'], self.data['pois'], chunk_size)
        return final_split_network, nearest, final_network_points

    def save_data_to_csv(self):
        # Create the path if it doesn't exist
        if not os.path.exists(self.path):
            os.makedirs(self.path)
            
        # Save dataframes to CSV
        edges_path = os.path.join(self.path, 'edges.csv')
        self.edges.to_csv(edges_path, sep=";", index=False)
        logger.info(f"Saved edges data to {edges_path}")
        
        nodes_path = os.path.join(self.path, 'nodes.csv')
        self.nodes.to_csv(nodes_path, sep=";", index=False)
        logger.info(f"Saved nodes data to {nodes_path}")
        
        relations_path = os.path.join(self.path, 'relations.csv')
        self.relations.to_csv(relations_path, sep=";", index=False)
        logger.info(f"Saved relations data to {relations_path}")
        
    def persist_to_db(self, **kwargs):
        # Check if the tables exist
        nodes_table = f"{self.keyname}_nodes"
        edges_table = f"{self.keyname}_edges"
        relations_table = f"{self.keyname}_relations"
        
        nodes_exist = self.db.table_exists(nodes_table, 'networks')
        edges_exist = self.db.table_exists(edges_table, 'networks')
        relations_exist = self.db.table_exists(relations_table, 'networks')
        
        # If not, create them
        if not all([nodes_exist, edges_exist, relations_exist]):
            logger.info("Creating tables in the database...")
            self.db.create_all(["nodes", "edges", "relations"], 'networks', self.keyname)
        
        # Now, let's upload the data from the CSVs
        nodes_path = os.path.join(self.path, 'nodes.csv')
        edges_path = os.path.join(self.path, 'edges.csv')
        relations_path = os.path.join(self.path, 'relations.csv')
        
        nodes_class = self.db.get_table_class("nodes", self.keyname)
        edges_class = self.db.get_table_class("edges", self.keyname)
        relations_class = self.db.get_table_class("relations", self.keyname)
        
        logger.info(f"Uploading data from {nodes_path} to the database...")
        self.db.add_data_from_csv(nodes_class, nodes_path)
        
        logger.info(f"Uploading data from {edges_path} to the database...")
        self.db.add_data_from_csv(edges_class, edges_path)
        
        logger.info(f"Uploading data from {relations_path} to the database...")
        self.db.add_data_from_csv(relations_class, relations_path)
        
        logger.info("Data uploaded to the database successfully.")
        
    def construct(self, **attributes):
        sources_exist, networks_exist = self.check_tables()

        if not sources_exist:
            logger.error("Source tables are missing. Can't proceed.")
            return

        if not networks_exist:
            logger.info("Network tables are missing. Will be built")
            self.make_tables()
            
        self.call_data()

        logger.info("Processing network from pois and road segments")
        
        self.edges, self.relations, self.nodes = self.process_network()

        self.save_data_to_csv()
        logger.info("Network processed and saved in datalake")
        
    def run(self, action: str, **kwargs) -> None:
        assert action in ['construct','persist','metrics','reset-database','reset-files'], "Specify a correct action for the network"
        
        logger.info(f"Calling network: {self.keyname}, Action: {action}")
        attributes = {**self.metadata, **kwargs, 'path': self.path}
        if action == 'construct':
            logger.info("Attempting to construct the network...")
            self.construct(**attributes)
        elif action == 'metrics':
            logger.info("Attempting to calculate the networks metrics...")
            self.metrics(**attributes)
        elif action == 'persist':
            logger.info("Attempting to persist the network...")
            self.persist_to_db(**attributes)