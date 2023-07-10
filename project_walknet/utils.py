from configparser import ConfigParser
import pandas as pd
from math import isnan
from os import remove
from sqlalchemy import create_engine
from sqlalchemy.sql import text

class DBInterface(): 
    
    def __init__(self,table,data,**kwargs):
        self.table = table
        self.data = data
        
    cfg = ConfigParser()
    cfg.read(r'./config.ini')
    
    global TEMP_PATH
    TEMP_PATH = cfg.get('DATALAKE','temp')
    
    
    DB_CONFIG = {
    'database':cfg.get('BBDD_CONNECTION','ddbb'),
    'user':cfg.get('BBDD_CONNECTION','user'),
    'password':cfg.get('BBDD_CONNECTION','password'),
    'host':cfg.get('BBDD_CONNECTION','host'),
    'port': cfg.get('BBDD_CONNECTION','port')}
    
    global DB_CONFIG_URL
    DB_CONFIG_URL = "postgresql://{user}:{password}@{host}/{database}".format(host=DB_CONFIG["host"],database=DB_CONFIG["database"],user = DB_CONFIG["user"], password = DB_CONFIG["password"])
    
    global connect
    def connect():
        return create_engine(DB_CONFIG_URL).connect()
    global raw_connect
    def raw_connect():
        return create_engine(DB_CONFIG_URL).raw_connection()

    
    global get_schema
    def get_schema(table):
        sql = text(f"""SELECT * FROM information_schema.columns WHERE table_name = '{table}'""")
        connection = connect()
        df = pd.read_sql(sql,con = connection)
        schema = df.sort_values(by='ordinal_position').set_index('column_name')[['udt_name','character_maximum_length']].to_dict(orient='index')
        connection.close()
        return schema
    
    global load_query
    def load_query(table, schema, temp):
        
        #temp = r"{p}\temp_{t}.csv".format(t=table,p=TEMP_PATH)
        
        def sql_create_schema(schema):
            concat = []
            for field, specs in schema.items():
                if field == 'geometry':
                    concat.append("geometry text NULL")
                else:
                    if not isnan(specs['character_maximum_length']):
                        concat.append(f"{field} {specs['udt_name']}({int(specs['character_maximum_length'])}) NULL")
                    else:
                        concat.append(f"{field} {specs['udt_name']} NULL")
            return ", \n".join(concat)
        
        sql_create_temp = f"""
        CREATE TABLE IF NOT EXISTS walknet.public.temp_{table} 
        ({sql_create_schema(schema)});"""
        
        sql_copy_temp = f"""COPY temp_{table} ({", ".join(list(schema.keys()))})
        FROM '{temp}'
        DELIMITER ','
        CSV HEADER;
        """
        
        if 'geometry' in schema.keys():
            sql_insert_into = f"""INSERT INTO {table} ({", ".join(list(schema.keys()))})
            SELECT
            {r", ".join([f'ST_GeomFromText({field},4326) geometry' if field == 'geometry' else field for field in list(schema.keys())])}
            FROM temp_{table} WHERE geometry IS NOT NULL;
            """
        else:
            sql_insert_into = f"""INSERT INTO {table} ({", ".join(list(schema.keys()))})
            SELECT
            {r", ".join([f'ST_GeomFromText({field},4326) geometry' if field == 'geometry' else field for field in list(schema.keys())])}
            FROM temp_{table};
            """
            
        sql_drop_temp = f"""
        DROP TABLE temp_{table};
        """
        
        return f"""{sql_create_temp}\n{sql_copy_temp}\n{sql_insert_into}\n{sql_drop_temp}"""
    
    def temp_to_db(self, data: pd.DataFrame):
        temp = r"{p}\temp_{t}.csv".format(t=self.table,p=TEMP_PATH)
        connection = raw_connect()
        cursor = connection.cursor()
        data.to_csv(temp, sep=";", index=False)
        query = load_query(self.table, get_schema(self.table))
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
        remove(temp)
        
    def safe_upload(self):
        from os import listdir
        from os.path import join
        data_path = r"{path}\level2".format(path = self.data)
        tables = [join(data_path,table) for table in listdir(data_path)]
        connection = raw_connect()
        
        for data_table in tables:
            cursor = connection.cursor()
            query = load_query(self.table, get_schema(self.table),data_table)
            cursor.execute(query)
            connection.commit()
        connection.close()
    #TAKES PARAMETERS AS KWARGS. CHECKS DATA IN THE TARGET TABLE. UPLOADS DATA. PERFORMS STATS. RETURNS REPORT
    
    

#TODO
#def reboot_schema() FOR INSTALLATION 
#def basic_statistics() FOR DATABASE ELEMENTS CONTROL