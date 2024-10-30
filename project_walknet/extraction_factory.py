import os
import yaml
import pandas as pd
from loguru import logger
from db_models import DBManager
from sqlalchemy import text
from configparser import ConfigParser

class Execution:
    cfg = ConfigParser()
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg.read(os.path.join(parent_dir, 'config.ini'))

    def __init__(self, keyname: str):
        self.keyname = keyname
        self.path = os.path.join(Execution.cfg.get('DATALAKE', 'path'), 'extractions')
        self.metadata = {}
        self.load_metadata()
        self.db = DBManager()
    
    def load_metadata(self):
        """Load metadata from executions.yaml."""
        try:
            with open("executions.yaml", "r", encoding="utf-8") as yaml_file:
                executions_metadata = yaml.safe_load(yaml_file)
                if self.keyname in executions_metadata:
                    self.metadata = executions_metadata[self.keyname]
                else:
                    logger.warning(f"Metadata for '{self.keyname}' not found in executions.yaml.")
        except FileNotFoundError:
            logger.warning("executions.yaml file not found.")
    
    def run_query(self, params=None):
        """Execute a SQL query from a SQL file and save the results to a CSV file."""
        # Set default params if not provided
        if params is None:
            params = {}

        # Path to the SQL file
        sql_file_path = os.path.join('..', "extractions", f"{self.keyname}.sql")
        
        # Format the output filename with params
        formatted_filename = f"{self.keyname}_" + "_".join(f"{k}_{v}" for k, v in params.items()) + ".csv"
        output_file_path = os.path.join(self.path, formatted_filename)

        # Ensure the output directory exists
        os.makedirs(self.path, exist_ok=True)

        try:
            with open(sql_file_path, 'r') as file:
                query = file.read()
            if params:
                query = query.format(**params)
            
            # Execute the query and save results
            result_df = pd.read_sql(text(query), self.db.engine)
            result_df.to_csv(output_file_path, index=False)
            logger.info(f"Query results saved to {output_file_path}")
        
        except FileNotFoundError as e:
            logger.error(f"SQL file not found: {sql_file_path}")
        
        except Exception as e:
            logger.error(f"Error executing query for {self.keyname}: {e}")

    def run(self, **kwargs):
        """Wrapper function to run queries based on the metadata."""
        logger.info(f"Running execution: {self.keyname}")
        self.run_query(params=self.metadata.get("params", {}))
