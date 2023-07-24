import os
import sys

# Add the parent directory to the system path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

import argparse
from project_walknet.source_factory import Source
from loguru import logger
from configparser import ConfigParser

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
    """
    Display parameters from the config.ini file.
    """
    # Initialize ConfigParser and read the config.ini file from the parent directory
    config_file_path = os.path.join(parent_dir, 'config.ini')
    config = ConfigParser()
    config.read(config_file_path)

    # Access and display parameters from the [PATHS_WN] section
    repository_path = config.get('PATHS_WN', 'REPOSITORY')

    # Access and display parameters from the [BBDD_CONNECTION] section
    db_name = config.get('BBDD_CONNECTION', 'ddbb')
    db_host = config.get('BBDD_CONNECTION', 'host')
    db_port = config.get('BBDD_CONNECTION', 'port')
    db_user = config.get('BBDD_CONNECTION', 'user')
    db_password = config.get('BBDD_CONNECTION', 'password')

    # Access and display parameters from the [DATALAKE] section
    datalake_path = config.get('DATALAKE', 'path')
    datalake_temp_path = config.get('DATALAKE', 'temp')

    # Mask the password with asterisks except for the first character
    masked_db_password = db_password[0] + '*' * (len(db_password) - 1)

    logger.info(f"Repository Path: {repository_path}")
    logger.info(f"Database Name: {db_name}")
    logger.info(f"Database Host: {db_host}")
    logger.info(f"Database Port: {db_port}")
    logger.info(f"Database User: {db_user}")
    logger.info(f"Database Password: {masked_db_password}")
    logger.info(f"Datalake Path: {datalake_path}")
    logger.info(f"Datalake Temp Path: {datalake_temp_path}")

def main():
    """
    Parse arguments and execute the specified action for the source.
    """
    parser = argparse.ArgumentParser(description="Run data gathering or processing for a specific source.")
    parser.add_argument("keyname", nargs="?", help="The keyname of the source.")
    parser.add_argument("action", nargs="?", help="The action to perform (gather/level0/level1/level2/persist).")
    parser.add_argument("--list-sources", action="store_true", help="List available sources and exit.")
    parser.add_argument("--config-params", action="store_true", help="Display parameters from config.ini and exit.", 
                        dest="config_params")  # Specify a dest name for the argument
    args = parser.parse_args()

    if args.list_sources:
        list_available_sources()
        sys.exit(0)

    if args.config_params:
        display_config_params()
        sys.exit(0)

    if not args.keyname or not args.action:
        parser.error("Both 'keyname' and 'action' arguments are required.")
    
    source = Source(keyname=args.keyname)
    source.run(args.action)

if __name__ == "__main__":
    main()
