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

    def persist(self, table, data):
        pass
        # Implement functionality to upload data
        
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
        
        edges, nearest, nodes = self.process_network()
        
        #TODO MAKE THIS PARAMETRIC!
        
        edges.to_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\edges.csv",sep=";",index=False)
        nearest.to_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\nearest.csv",sep=";",index=False)
        nodes.to_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\nodes.csv",sep=";",index=False)
        
        logger.info("Network processed and saved in datalake")
        
        #MAKE ANOTHER PARAMETER SUCH AS PERSIST OR NOT?
            
        
        """
        if self.check_tables():
            make_queries()
            load_queries()
            #process it 
            #upload it
            pass
            #table creation with prefixes
            
        else: 
            make the schema tables
            query the data and load it to the network object
            process it 
            upload it
        
        # Define the sequence in which the methods will be called to construct the network.
        # Example:
        # self.call_data()
        # self.make_network()
        # self.upload(table="some_table", data=self.nodes)
        """
    def run(self, action: str, **kwargs) -> None:
        assert action in ['construct','persist','metrics'], "Specify a correct action for the network"
        
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
            self.persist(**attributes)