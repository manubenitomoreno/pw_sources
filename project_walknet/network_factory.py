import os
import yaml
from configparser import ConfigParser
from loguru import logger
from typing import Optional
from db_models import DBManager
from sqlalchemy import text
import pandas as pd
from networks.make_network import make_network
from networks.shortest_paths import make_shortest_paths
from shapely import wkt, wkb
import geopandas as gpd

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
        self.paths = gpd.GeoDataFrame()

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
            self.metadata.get('pois_table')#,
            #self.metadata.get('extent_table')
        ])

        network_tables = [f"{self.keyname}_nodes", f"{self.keyname}_edges", f"{self.keyname}_relations"]
        networks_exist = all(self.db.table_exists(table, self.keyname) for table in network_tables)
        
        return sources_exist, networks_exist
                
    def make_tables(self):
        table_names = [
        "nodes",
        "edges",
        "relations",
        "paths"]
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
        pois_query = f"SELECT * FROM sources.{pois_table} s WHERE {pois_where}"
        
        #spatial_join = """ST_INTERSECTS(ST_Transform(st_setsrid(st_geomfromewkt(st_asewkt(s.geometry)),25830), 25830),st_setsrid(st_geomfromewkt(st_asewkt(e.geometry)),25830))"""
        spatial_join = """ST_INTERSECTS(s.geometry,e.geometry)"""
        
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
                df = pd.DataFrame.from_dict(self.db.get_query_results(text(road_segments_query)))
                
                df['geometry'] = df['geometry'].apply(wkt.loads)
                self.data['road_segments'] = gpd.GeoDataFrame(
                    df,
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
                df = pd.DataFrame.from_dict(self.db.get_query_results(text(pois_query)))
                try:
                    df['geometry'] = df['geometry'].apply(wkt.loads)
                except:
                    df['geometry'] = df['geometry'].apply(wkb.loads)
                self.data['pois'] = gpd.GeoDataFrame(
                    df,
                    geometry='geometry',
                    crs = "EPSG:25830")
            logger.info("POIs data ready")
            
    def call_network_tables(self, tables):
        
        return {table: pd.DataFrame.from_dict(
            self.db.get_query_results(text(f"SELECT * FROM networks.{self.keyname}_{table}"))) for table in tables}
            
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
        
        for id in self.edges['provider'].unique():
            edges_path = os.path.join(self.path, f'edges_{str(id)}.csv')
            self.edges.loc[self.edges['provider'] == id].to_csv(edges_path, sep=";", index=False)
            logger.info(f"Saved edges data to {edges_path}")
        
        for id in self.nodes['provider'].unique():
            nodes_path = os.path.join(self.path, f'nodes_{str(id)}.csv')
            self.nodes.loc[self.nodes['provider'] == id].to_csv(nodes_path, sep=";", index=False)
            logger.info(f"Saved nodes data to {nodes_path}")
        
        for id in self.relations['provider'].unique():
            relations_path = os.path.join(self.path, f'relations_{str(id)}.csv')
            self.relations.loc[self.relations['provider'] == id].to_csv(relations_path, sep=";", index=False)
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
        nodes_files = [f for f in os.listdir(self.path) if f.startswith('nodes')]
        edges_files = [f for f in os.listdir(self.path) if f.startswith('edges')]
        relations_files = [f for f in os.listdir(self.path) if f.startswith('relations')]

        for id in [f.split("_")[1].split(".")[0] for f in nodes_files]:
            nodes_path = os.path.join(self.path, f'nodes_{id}.csv')
            nodes_class = self.db.get_table_class("nodes", self.keyname)
            logger.info(f"Uploading data from {nodes_path} to the database...")
            self.db.add_data_from_csv(nodes_class, nodes_path)
        
        for id in [f.split("_")[1].split(".")[0] for f in edges_files]:
            edges_path = os.path.join(self.path, f'edges_{id}.csv')
            edges_class = self.db.get_table_class("edges", self.keyname)
            logger.info(f"Uploading data from {edges_path} to the database...")
            self.db.add_data_from_csv(edges_class, edges_path)

        for id in [f.split("_")[1].split(".")[0] for f in relations_files]:
            relations_path = os.path.join(self.path, f'relations_{id}.csv')
            relations_class = self.db.get_table_class("relations", self.keyname)
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
        
    def metrics(self, **attributes):
        
        if 'shortest_paths' in self.metadata['metrics']:
            tables = self.call_network_tables(['edges'])
            
            #tables['edges'] = tables['edges'].drop(columns='geometry')
            logger.info("Computing shortest paths")
            data_tables = make_shortest_paths(tables['edges'])
            
            #paths_table = f"{self.keyname}_paths"
            #paths_exist = self.db.table_exists(paths_table, 'networks')
            
            length_table = f"{self.keyname}_length"
            length_exist = self.db.table_exists(length_table, 'networks')

            ego_table = f"{self.keyname}_ego"
            ego_exist = self.db.table_exists(ego_table, 'networks')
                
            #if not paths_exist:
                #logger.info("Creating tables in the database...")
                #self.db.create_paths_table(self.keyname)
            if not length_exist:
                logger.info("Creating tables in the database...")
                self.db.create_length_table(self.keyname)
            if not ego_exist:
                logger.info("Creating tables in the database...")
                self.db.create_ego_table(self.keyname)
            """
            for id in data_tables['path_nodes'].loc[data_tables['path_nodes']['provider'].notnull()]['provider'].unique():
                paths_nodes_path = os.path.join(self.path, f'paths_nodes_{str(int(id))}.csv')
                data_tables['path_nodes'][data_tables['path_nodes']['provider']==id].to_csv(paths_nodes_path, sep=";", index=False)
                logger.info(f"Saved paths data to {paths_nodes_path}")
                paths_class = self.db.get_table_class("paths", self.keyname)
                logger.info(f"Uploading data from {paths_nodes_path} to the database...")
                self.db.add_data_from_csv(paths_class, paths_nodes_path)
            """

            for id in data_tables['length'].loc[data_tables['length']['provider'].notnull()]['provider'].unique():
                length_path = os.path.join(self.path, f'length_{str(int(id))}.csv')
                data_tables['length'][data_tables['length']['provider']==id].to_csv(length_path, sep=";", index=False)
                logger.info(f"Saved paths length data to {length_path}")
                length_class = self.db.get_table_class("length", self.keyname)
                logger.info(f"Uploading data from {length_path} to the database...")
                self.db.add_data_from_csv(length_class, length_path)

            for id in data_tables['ego_graphs'].loc[data_tables['ego_graphs']['provider'].notnull()]['provider'].unique():
                ego_graphs_path = os.path.join(self.path, f'ego_graphs_{str(int(id))}.csv')
                data_tables['ego_graphs'][data_tables['ego_graphs']['provider']==id].to_csv(ego_graphs_path, sep=";", index=False)
                logger.info(f"Saved ego graphs data to {ego_graphs_path}")
                ego_class = self.db.get_table_class("ego", self.keyname)
                logger.info(f"Uploading data from {ego_graphs_path} to the database...")
                self.db.add_data_from_csv(ego_class, ego_graphs_path)
                    
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