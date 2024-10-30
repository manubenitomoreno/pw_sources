import warnings
warnings.filterwarnings("ignore")

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
from project_walknet.extraction_factory import Execution
from project_walknet.db_models import DBManager, Base
from project_walknet.run_statistics import generate_table_statistics, generate_datalake_statistics, initialize_execution_statistics, generate_execution_statistics
from loguru import logger
from configparser import ConfigParser

import time
pipeline_logger = logger.bind(pipeline='')
def list_available_sources():
    """List available sources based on sources.yaml."""
    try:
        import yaml
        with open("sources.yaml", "r") as yaml_file:
            sources_metadata = yaml.safe_load(yaml_file)
        logger.info("Available sources:")
        for source_name in sources_metadata:
            logger.info(f"- {source_name}")
    except FileNotFoundError:
        logger.warning("sources.yaml file not found.")

def list_available_extractions():
    """List available extractions based on executions.yaml."""
    try:
        import yaml
        with open("extractions.yaml", "r") as yaml_file:
            executions_metadata = yaml.safe_load(yaml_file)
        logger.info("Available executions:")
        for execution_name in extractions_metadata:
            logger.info(f"- {extraction_name}")
    except FileNotFoundError:
        logger.warning("extractions.yaml file not found.")

def display_config_params():
    """Display database and repository config parameters from config.ini."""
    config_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config = ConfigParser()
    config.read(config_file_path)
    
    repository_path = config.get('PATHS_WN', 'REPOSITORY')
    host = config.get('BBDD_CONNECTION', 'host')
    port = config.get('BBDD_CONNECTION', 'port')
    user = config.get('BBDD_CONNECTION', 'user')
    database = config.get('BBDD_CONNECTION', 'ddbb')
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
    """Parse arguments and execute the specified action for source, network, or execution."""
    parser = argparse.ArgumentParser(description="Run data pipelines for sources, networks, or execute SQL queries.")
    
    # Define mutually exclusive groups for Source, Network, and Execution
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--source", action="store_true", help="Indicate that the operation is for a source.")
    group.add_argument("-n", "--network", action="store_true", help="Indicate that the operation is for a network.")
    group.add_argument("-e", "--extraction", action="store_true", help="Indicate that the operation is for an extraction.")

    parser.add_argument("keyname", nargs="?", help="The keyname of the source, network, or extraction.")
    parser.add_argument("action", nargs="?", help="The action to perform (e.g., gather, persist, run).")
    parser.add_argument("--list-sources", action="store_true", help="List available sources and exit.")
    parser.add_argument("--list-executions", action="store_true", help="List available extractions and exit.")
    parser.add_argument("--config-params", action="store_true", help="Display config parameters and exit.", dest="config_params")
    parser.add_argument("--reset-database", action="store_true", help="Reset the database and exit.", dest="reset_database")
    parser.add_argument("--reset-source", nargs="?", const="all", default=None, help="Reset a source's files in the datalake", dest="reset_source")
    
    args = parser.parse_args()

    if args.list_sources:
        list_available_sources()
        sys.exit(0)

    if args.list_executions:
        list_available_extractions()
        sys.exit(0)

    if args.config_params:
        display_config_params()
        sys.exit(0)
    
    if args.reset_database:
        response = input("Are you sure you want to reset the sources database? Y/N: ")
        if response.lower() == "y":
            db = DBManager()
            db.drop_all_tables('sources')
            db.create_all(['pois', 'aois', 'road_segments', 'boundaries_geo', 'boundaries_data', 'other_data'], 'sources')
            print("Database reset successfully.")
        else:
            print("Database reset cancelled.")
        sys.exit(0)

    # Reset source data in the datalake
    if args.reset_source:
        datalake_path = Source.cfg.get('DATALAKE', 'path')
        if args.reset_source.lower() == 'all':
            response = input("Are you sure you want to delete all files in the Datalake? Y/N: ")
            if response.lower() == 'y':
                shutil.rmtree(datalake_path)
                os.mkdir(datalake_path)
                print("Datalake reset successfully.")
            else:
                print("Datalake reset cancelled.")
        else:
            source_dir = os.path.join(datalake_path, args.reset_source)
            if os.path.isdir(source_dir):
                response = input(f"Are you sure you want to delete all files for source {args.reset_source}? Y/N: ")
                if response.lower() == 'y':
                    shutil.rmtree(source_dir)
                    os.mkdir(source_dir)
                    print(f"Source {args.reset_source} reset successfully.")
                else:
                    print(f"Reset of source {args.reset_source} cancelled.")
            else:
                print(f"Source {args.reset_source} not found.")
        sys.exit(0)
      
    # Handle Source operations
    if args.source:
        if args.keyname and args.action:
            source = Source(keyname=args.keyname)
            start_time = time.time()
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            source.run(args.action)
            execution_time = time.time() - start_time
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing finished in {execution_time:.2f} seconds.")
        else:
            print("Please provide both keyname and action for the source pipeline.")

    # Handle Network operations
    elif args.network:
        if args.keyname and args.action:
            network = Network(keyname=args.keyname)
            start_time = time.time()
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            network.run(args.action)
            execution_time = time.time() - start_time
            pipeline_logger.bind(pipeline=args.action).info(f"{args.keyname} - Processing finished in {execution_time:.2f} seconds.")
        else:
            print("Please provide both keyname and action for the network pipeline.")

    # Handle Extraction operations
    elif args.extraction:
        if args.keyname:
            execution = Execution(keyname=args.keyname)
            start_time = time.time()
            pipeline_logger.bind(pipeline="extraction").info(f"{args.keyname} - Extraction started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            execution.run()
            execution_time = time.time() - start_time
            pipeline_logger.bind(pipeline="extraction").info(f"{args.keyname} - Extraction finished in {execution_time:.2f} seconds.")
        else:
            print("Please provide a keyname for the extraction.")

    else:
        parser.error("Please specify --source, --network, or --extraction with a keyname and action, or use --list-sources, --list-executions, --config-params, or --reset-* options.")
      
if __name__ == "__main__":
    main()
