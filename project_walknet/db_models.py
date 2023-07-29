from sqlalchemy import Column, Integer, String, create_engine, inspect, Sequence, Table, MetaData, ForeignKey, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from configparser import ConfigParser
import os

from geoalchemy2 import Geometry

import pandas as pd

Base = declarative_base()

class POIs(Base):
    __tablename__ = 'pois'

    id = Column(String(300), primary_key=True)
    id_class = Column(String(20))
    category = Column(String(50))
    provider = Column(Integer())
    data = Column(JSON)
    geometry = Column(Geometry(geometry_type='POINT'))
    
class AOIs(Base):
    __tablename__ = 'aois'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSON)
    geometry = Column(Geometry(geometry_type='POLYGON'))

class RoadSegments(Base):
    __tablename__ = 'road_segments'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSON)
    geometry = Column(Geometry(geometry_type='LINESTRINGZ'))
    
class BoundariesGeo(Base):
    __tablename__ = 'boundaries_geo'

    id = Column(String(300), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSON)
    geometry = Column(Geometry(geometry_type='MULTIPOLYGON'))

class BoundariesData(Base):
    __tablename__ = 'boundaries_data'

    id = Column(String(300), primary_key=True)
    geo_id = Column(String(300), ForeignKey('boundaries_geo.id')) # this is the foreign key
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSON)
    
class OtherData(Base):
    __tablename__ = 'other_data'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    id_class = Column(String(50))
    category = Column(String(50))
    provider = Column(String(50))
    data = Column(JSON)

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


    def create_all(self):
        Base.metadata.create_all(self.engine)

    def add_data_from_csv(self, table_class, csv_file_path):
        data = pd.read_csv(csv_file_path, sep = ";")
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
        
    def table_exists(self, table_name):
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)
    
    
    def drop_all_tables(self):
        from sqlalchemy import text
        DROP_ALL_TABLES_QUERY = """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            -- if the schema you operate on is not "public", you can
            -- replace "public" with your schema name
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
        """
        self.session.execute(text(DROP_ALL_TABLES_QUERY))
        self.session.commit()
        
    
    def get_table_class(self, table_name):
        if table_name == 'pois':
            return POIs
        elif table_name == 'aois':
            return AOIs
        elif table_name == 'road_segments':
            return RoadSegments
        elif table_name == 'boundaries_geo':
            return BoundariesGeo
        elif table_name == 'boundaries_data':
            return BoundariesData
        elif table_name == 'other_data':
            return OtherData
        else:
            raise ValueError(f"Unrecognized table name: {table_name}")

        
