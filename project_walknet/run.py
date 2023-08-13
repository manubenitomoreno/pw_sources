import os
import sys
import shutil
import json
from sqlalchemy import inspect, text
from collections import defaultdict

# Add the parent directory to the system path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

import argparse
from project_walknet.source_factory import Source
from project_walknet.network_factory import Network
from project_walknet.db_models import DBManager, Base
from project_walknet.run_statistics import generate_table_statistics, generate_datalake_statistics, initialize_execution_statistics, generate_execution_statistics
from loguru import logger
from configparser import ConfigParser

import time
pipeline_logger = logger.bind(pipeline='')


def list_available_sources():
    """
    List available sources based on the sources.yaml file.
    """
    try:
        import yaml
        with open("sources.yaml", "r") as yaml_file:
            sources_metadata = yaml.safe_load(yaml_file)

        logger.info("Available sources:")
        for source_name in sources_metadata:
            logger.info(f"- {source_name}")
    except FileNotFoundError:
        logger.warning("sources.yaml file not found.")
        
def display_config_params():
    # Initialize ConfigParser and read the config.ini file from the parent directory
    config_file_path = os.path.join(parent_dir, 'config.ini')
    config = ConfigParser()
    config.read(config_file_path)

    # Access and display parameters from the [PATHS_WN] section
    repository_path = config.get('PATHS_WN', 'REPOSITORY')

    # Access and display parameters from the [BBDD_CONNECTION] section
    host = config.get('BBDD_CONNECTION', 'host')
    port = config.get('BBDD_CONNECTION', 'port')
    user = config.get('BBDD_CONNECTION', 'user')
    database = config.get('BBDD_CONNECTION', 'ddbb')

    # Access and display parameters from the [DATALAKE] section
    datalake_path = config.get('DATALAKE', 'path')
    datalake_temp_path = config.get('DATALAKE', 'temp')

    logger.info(f"Repository Path: {repository_path}")
    logger.info(f"Database Host: {host}")
    logger.info(f"Database Port: {port}")
    logger.info(f"Database User: {user}")
    logger.info(f"Database Name: {database}")
    logger.info(f"Datalake Path: {datalake_path}")
    logger.info(f"Datalake Temp Path: {datalake_temp_path}")


def main():
    """
    Parse arguments and execute the specified action for the source.
    """
    parser = argparse.ArgumentParser(description="Run data gathering, processing for a specific source, handle network operations, or invoke specific extractions")
    # Define mutually exclusive groups
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--source", action="store_true", help="Indicate that the operation is for a source.")
    group.add_argument("-n", "--network", action="store_true", help="Indicate that the operation is for a network.")
    
    parser.add_argument("keyname", nargs="?", help="The keyname of the source, network, or extraction.")
    parser.add_argument("action", nargs="?", help="The action to perform (gather/level0/level1/level2/persist).") #TODO detailed for the three options
    
    parser.add_argument("--list-sources", action="store_true", help="List available sources and exit.")
    parser.add_argument("--config-params", action="store_true", help="Display parameters from config.ini and exit.", 
                        dest="config_params")
    parser.add_argument("--reset-all-sources", action="store_true", help="Reset the database and exit.", 
                        dest="reset_all_sources")
    parser.add_argument("--reset-source", nargs="?", const="all", default=None, 
                        help="Reset a source (delete all its files) or all sources. Provide source keyname or 'all'.", 
                        dest="reset_source")
    
    args = parser.parse_args()

    if args.list_sources:
        list_available_sources()
        sys.exit(0)

    if args.config_params:
        display_config_params()
        sys.exit(0)
        
    if args.reset_all_sources:
        response = input("Are you sure you want to reset the sources database? This will delete all data in sources. Y/N: ")
        if response.lower() == "y":
            db = DBManager()
            db.drop_all_tables('sources')  # This will drop all tables in the public schema
            db.create_all('sources')  # Create all tables based on your SQLAlchemy models
            print("Database reset successfully.")
        else:
            print("Database reset cancelled.")
        sys.exit(0)

    #TODO I don't think this is ever working but it is not critical
    if args.reset_source:
        datalake_path = Source.cfg.get('DATALAKE', 'path')
        if args.reset_source.lower() == 'all':
            response = input("Are you sure you want to delete all files in the Datalake? This will delete all source data. Y/N: ")
            if response.lower() == 'y':
                # Delete the datalake directory entirely
                shutil.rmtree(datalake_path)
                # Recreate the empty datalake directory
                os.mkdir(datalake_path)
                print("Datalake reset successfully.")
            else:
                print("Datalake reset cancelled.")
        else:
            # Delete a specific source directory in the datalake
            source_dir = os.path.join(datalake_path, args.reset_source)
            if os.path.isdir(source_dir):
                response = input(f"Are you sure you want to delete all files for source {args.reset_source} in the Datalake? This will delete the source's data. Y/N: ")
                if response.lower() == 'y':
                    # Delete the source directory
                    shutil.rmtree(source_dir)
                    # Recreate the empty source directory
                    os.mkdir(source_dir)
                    print(f"Source {args.reset_source} reset successfully.")
                else:
                    print(f"Reset of source {args.reset_source} cancelled.")
            else:
                print(f"Source {args.reset_source} not found in the Datalake.")
        sys.exit(0)
      
    if args.source: 
        if args.keyname and args.action:
            
            execution_statistics_file = os.path.join(parent_dir, 'execution_statistics.json')
            initialize_execution_statistics(execution_statistics_file)
            
            source = Source(keyname=args.keyname)
            
            # Log the start time of the pipeline run
            start_time = time.time()
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

            source.run(args.action)
            
            end_time = time.time()
            execution_time = end_time - start_time
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}, Time Spent: {execution_time:.2f} seconds")
            
            # Generate statistics for datalake and database
            config = ConfigParser()
            config.read(os.path.join(parent_dir, 'config.ini'))
            datalake_path = config.get('DATALAKE', 'path')
            generate_datalake_statistics(datalake_path)
            db_manager = DBManager()
            generate_table_statistics(db_manager)
            db_manager.close()
            generate_execution_statistics(args.keyname, args.action, execution_time, execution_statistics_file)
            
            pipeline_logger.info(f"Refreshing stats...")
            
        else: print("Please provide both keyname and action for source pipeline")
        
    elif args.network:
        if args.keyname and args.action:
            
            execution_statistics_file = os.path.join(parent_dir, 'execution_statistics.json')
            initialize_execution_statistics(execution_statistics_file)
            
            network = Network(keyname=args.keyname)
            
            # Log the start time of the network workflow run
            start_time = time.time()
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            network.run(args.action)
            
            end_time = time.time()
            execution_time = end_time - start_time
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}, Time Spent: {execution_time:.2f} seconds")
            
            # Generate statistics for datalake and database
            config = ConfigParser()
            config.read(os.path.join(parent_dir, 'config.ini'))
            datalake_path = config.get('DATALAKE', 'path')
            generate_datalake_statistics(datalake_path)
            db_manager = DBManager()
            generate_table_statistics(db_manager)
            db_manager.close()
            generate_execution_statistics(args.keyname, args.action, execution_time, execution_statistics_file)
            
            pipeline_logger.info(f"Refreshing stats...")
            
        else: print("Please provide both keyname and action for network workflow")
        
    #elif args.execution:
        #if args.keyname and args.action:
        
    elif not args.list_sources and not args.config_params and not args.reset_db and not args.reset_source:
        parser.error("Either --keyname and --action, or one of --list-sources, --config-params, --reset-all-sources, --reset-source must be provided.") #TODO update

if __name__ == "__main__":
    main()
    