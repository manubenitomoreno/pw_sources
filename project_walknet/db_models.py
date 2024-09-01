from sqlalchemy import Column, Integer, String, create_engine, inspect, Sequence, Table, MetaData, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

from configparser import ConfigParser
import os

from geoalchemy2 import Geometry

import pandas as pd

Base = declarative_base()

class POIs(Base):
    __tablename__ = 'pois'
    __table_args__ = {'schema': 'sources'}

    id = Column(String(300), primary_key=True)
    id_class = Column(String(20))
    category = Column(String(50))
    provider = Column(Integer())
    data = Column(JSONB)
    geometry = Column(Geometry(geometry_type='POINT'))
    
class AOIs(Base):
    __tablename__ = 'aois'
    __table_args__ = {'schema': 'sources'}

    id = Column(String(300), primary_key=True)
    id_class = Column(String(20))
    category = Column(String(50))
    provider = Column(String())
    data = Column(JSONB)
    geometry = Column(Geometry(geometry_type='MULTIPOLYGON'))

class RoadSegments(Base):
    __tablename__ = 'road_segments'
    __table_args__ = {'schema': 'sources'}

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSONB)
    geometry = Column(Geometry(geometry_type='LINESTRINGZ'))
    
class BoundariesGeo(Base):
    __tablename__ = 'boundaries_geo'
    __table_args__ = {'schema': 'sources'}

    id = Column(String(300), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSONB)
    geometry = Column(Geometry(geometry_type='MULTIPOLYGON'))

class BoundariesData(Base):
    __tablename__ = 'boundaries_data'
    __table_args__ = {'schema': 'sources'}

    id = Column(String(300), primary_key=True)
    geo_id = Column(String(300)) # this is the foreign key
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSONB)
    
class OtherData(Base):
    __tablename__ = 'other_data'
    __table_args__ = {'schema': 'sources'}

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSONB)

def create_nodes_table(prefix=""):
    #if f"{prefix}Nodes" in globals():
        #return globals()[f"{prefix}Nodes"]
    class Nodes(Base):
        __tablename__ = f'{prefix}_nodes' if prefix else 'nodes'
        __table_args__ = {'schema': 'networks'}
        node_id = Column(Integer, primary_key=True)
        geometry = Column(Geometry(geometry_type='POINT'))

    return Nodes

def create_edges_table(prefix=""):
    #if f"{prefix}Nodes" in globals():
        #return globals()[f"{prefix}Nodes"]
    class Edges(Base):
        __tablename__ = f'{prefix}_edges' if prefix else 'edges'
        __table_args__ = {'schema': 'networks'}
        edge_id = Column(Integer, primary_key=True)
        data = Column(JSONB)
        start = Column(Integer)
        end = Column(Integer)
        geometry = Column(Geometry(geometry_type='LINESTRING'))

    return Edges

def create_relations_table(prefix=""):
    #if f"{prefix}Nodes" in globals():
        #return globals()[f"{prefix}Nodes"]
    class Relations(Base):
        __tablename__ = f'{prefix}_relations' if prefix else 'relations'
        __table_args__ = {'schema': 'networks'}
        relation_id = Column(String(300), primary_key=True)
        relation_kind = Column(String(300))
        data = Column(JSONB)

    return Relations

def create_paths_table(prefix=""):
    class Paths(Base):
        __tablename__ = f'{prefix}_paths' if prefix else 'paths'
        __table_args__ = {'schema': 'networks'}
        relation_id = Column(String(300), primary_key=True)
        relation_kind = Column(String(300))
        data = Column(JSONB)

    return Paths

class DBManager:
    def __init__(self):
        # Initialize ConfigParser and read the config.ini file from the parent directory
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_file_path = os.path.join(parent_dir, 'config.ini')
        config = ConfigParser()
        config.read(config_file_path)
        
        
        # Get the database connection parameters from config
        user = config.get('BBDD_CONNECTION', 'user')
        password = config.get('BBDD_CONNECTION', 'password')
        host = config.get('BBDD_CONNECTION', 'host')
        port = config.get('BBDD_CONNECTION', 'port')
        database = config.get('BBDD_CONNECTION', 'ddbb')
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        self.session = sessionmaker(bind=self.engine)()
        
    def create_all(self, table_names, target_schema, prefix=""):
        # Convert table names to table classes
        table_classes = [self.get_table_class(table_name) for table_name in table_names]
            
        if target_schema == 'sources':
            tables_to_create = [table.metadata.tables.get(f"{table.__table_args__['schema']}.{table.__tablename__}") for table in table_classes]
            Base.metadata.create_all(self.engine, tables=tables_to_create)
        elif target_schema == 'networks':
            # Create the NODES, EDGES AND RELATIONS TABLES, with the provided prefix
            Nodes = create_nodes_table(prefix)
            Edges = create_edges_table(prefix)
            Relations = create_relations_table(prefix)
            Paths = create_paths_table(prefix)
            
            # Add them to Base metadata
            tables_to_create = [
                Nodes.metadata.tables.get(f"{Nodes.__table_args__['schema']}.{Nodes.__tablename__}"),
                Edges.metadata.tables.get(f"{Edges.__table_args__['schema']}.{Edges.__tablename__}"),
                Relations.metadata.tables.get(f"{Relations.__table_args__['schema']}.{Relations.__tablename__}"),
                Paths.metadata.tables.get(f"{Paths.__table_args__['schema']}.{Paths.__tablename__}"),
            ]
            Base.metadata.create_all(self.engine, tables=tables_to_create)

    def add_data_from_csv(self, table_class, csv_file_path):
        import json
        data = pd.read_csv(csv_file_path, sep = ";",encoding='latin-1')
        
        if 'data' in data.columns:
            
            
            data['data'] = data['data'].astype(str).str.replace("'",'"')
            try: 
                data['data'] = data['data'].apply(json.loads)
            except Exception as e:
                print(e)
                pass
 
        data_dict = data.to_dict(orient='records')

        try:
            self.session.bulk_insert_mappings(table_class, data_dict)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Failed to upload CSV file. Error: {str(e)}")
            return

    def add_data_from_dataframe(self, table_class, dataframe):
        data_dict = dataframe.to_dict(orient='records')

        try:
            self.session.bulk_insert_mappings(table_class, data_dict)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Failed to upload DataFrame. Error: {str(e)}")
            return

    def get_query_results(self, query):
        try:
            return self.session.execute(query).fetchall()
        except SQLAlchemyError as e:
            print(f"Failed to execute query. Error: {str(e)}")
            return None

    def close(self):
        self.session.close()
        
    def table_exists(self, table_name, target_schema):
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names(schema=target_schema)
    
    def drop_all_tables(self, target_schema):
        #from sqlalchemy import text
        print(f"Attempting to drop all tables in schema: {target_schema}")
        DROP_ALL_TABLES_QUERY = f"""
        DO $$ DECLARE
            r RECORD;
        BEGIN
            
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '{target_schema}') LOOP
                EXECUTE 'DROP TABLE IF EXISTS {target_schema}.' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
        """
        try:
            self.session.execute(text(DROP_ALL_TABLES_QUERY))
            self.session.commit()
            print(f"Tables in schema {target_schema} dropped successfully.")
        except Exception as e:
            print(f"Error dropping tables: {e}")
            self.session.rollback()
        
    def get_table_class(self, table_name, prefix=""):
        static_table_mapping = {
                'pois': POIs,
                'aois': AOIs,
                'road_segments': RoadSegments,
                'boundaries_geo': BoundariesGeo,
                'boundaries_data': BoundariesData,
                'other_data': OtherData
            }

        # If it's one of the dynamic tables, then generate it using the prefix
        if table_name in ["nodes", "edges", "relations", "paths"]:
            if table_name == "nodes":
                return create_nodes_table(prefix)
            elif table_name == "edges":
                return create_edges_table(prefix)
            elif table_name == "relations":
                return create_relations_table(prefix)
            elif table_name == "paths":
                return create_paths_table(prefix)
        
        # Otherwise, use the static mapping
        return static_table_mapping.get(table_name) or ValueError(f"Unrecognized table name: {table_name}")

        
    def retrieve_data(self, table_name, filters=None):
        """
        Retrieve data from a specified table.
        
        Parameters:
        - table_name (str): Name of the table from which to retrieve data.
        - filters (dict, optional): Conditions to apply to the query.
        
        Returns:
        - List[Base]: List of model instances retrieved from the table.
        """
        table_class = self.get_table_class(table_name)

        query = self.session.query(table_class)
        
        if filters:
            for column, value in filters.items():
                query = query.filter(getattr(table_class, column) == value)
        
        return query.all()

    def delete_network_data(self, network_name):
        """Delete all data for a specific network in the database."""
        # The table names associated with this network
        table_names = [f"{network_name}_nodes", f"{network_name}_edges", f"{network_name}_relations", f"{network_name}_paths"]

        for table_name in table_names:
            if self.table_exists(table_name, target_schema="networks"):
                try:
                    # Drop each table associated with this network
                    table_class = self.get_table_class(table_name)
                    table_class.__table__.drop(self.engine)
                    print(f"Table {table_name} dropped successfully.")
                except SQLAlchemyError as e:
                    print(f"Failed to drop table {table_name}. Error: {str(e)}")
            else:
                print(f"Table {table_name} not found.")


        
