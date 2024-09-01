import os
import sys
import json
from sqlalchemy import inspect, text
from collections import defaultdict

# Add the parent directory to the system path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from loguru import logger

import time
pipeline_logger = logger.bind(pipeline='')

def generate_execution_statistics(source_name, action, execution_time, json_file_path):
    """
    Update the existing JSON file with execution time for the specified action of a source.

    Parameters:
        source_name (str): The name of the source.
        action (str): The action that was executed for the source (gather, level0, level1, persist).
        execution_time (float): The execution time for the action in seconds.
        json_file_path (str): The file path to the existing JSON file that will be updated.
    """
    try:
        # Read the existing JSON file
        with open(json_file_path, 'r') as json_file:
            existing_data = json.load(json_file)
    except FileNotFoundError:
        # If the file does not exist, create an empty dictionary
        existing_data = {}

    # Update the existing data with the execution time for the source and action
    if source_name not in existing_data:
        existing_data[source_name] = {}

    existing_data[source_name][action] = execution_time

    # Write the updated data back to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=2)

def initialize_execution_statistics(json_file_path):
    """
    Initialize the execution statistics JSON file with an empty dictionary.

    Parameters:
        json_file_path (str): The file path to the execution statistics JSON file.
    """
    try:
        # Try reading the existing JSON file
        with open(json_file_path, 'r') as json_file:
            existing_data = json.load(json_file)
    except FileNotFoundError:
        # If the file does not exist, create an empty dictionary
        existing_data = {}

    # Write the updated data (with potentially new source keys) back to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=2)

def calculate_data_stats(datalake_path):
    datalake_path = os.path.join(datalake_path,'sources')
    data_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for source_name in os.listdir(datalake_path):
        source_path = os.path.join(datalake_path, source_name)
        if os.path.isdir(source_path) and source_name != 'temp':
            for level_folder in os.listdir(source_path):
                if not level_folder in ['level0','level1','level2']:
                    pass
                else:
                    level_path = os.path.join(source_path, level_folder)
                    if os.path.isdir(level_path):
                        level_stats = {
                            'size_bytes': 0,
                            'size_kb': 0.0,
                            'size_mb': 0.0,
                            'total_files': 0
                        }
                        for root, _, files in os.walk(level_path):
                            for file_name in files:
                                file_path = os.path.join(root, file_name)
                                data_size = os.path.getsize(file_path)

                                # Update statistics
                                level_stats['size_bytes'] += data_size
                                level_stats['size_kb'] = level_stats['size_bytes'] / 1024
                                level_stats['size_mb'] = level_stats['size_bytes'] / (1024 * 1024)
                                level_stats['total_files'] += 1

                        # Update data_stats with the level_stats for the current level_folder
                        data_stats[source_name][level_folder] = level_stats

    return data_stats

def generate_datalake_statistics(datalake_path):
    datalake_statistics = calculate_data_stats(datalake_path)
    # Save the statistics to a JSON file
    statistics_file = os.path.join(parent_dir, 'datalake_statistics.json')
    with open(statistics_file, 'w') as json_file:
        json.dump(datalake_statistics, json_file, indent=2)

def generate_table_statistics(db_manager):
    table_statistics = {}
    inspector = inspect(db_manager.engine)
    for table_name in inspector.get_table_names():
        if table_name in ['spatial_ref_sys','pointcloud_formats','us_gaz','us_lex','us_rules','topology','layer','cousub']:
            continue
        table_class = db_manager.get_table_class(table_name)
        row_count = db_manager.session.query(table_class).count()
        query = text(f"SELECT pg_total_relation_size('{table_name}')")
        data_size_bytes = db_manager.session.execute(query).scalar()

        table_statistics[table_name] = {
            'row_count': row_count,
            'data_size_bytes': data_size_bytes,
            'data_size_kb': data_size_bytes / 1024,
            'data_size_mb': data_size_bytes / (1024 * 1024),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        # Save the statistics to a JSON file
    statistics_file = os.path.join(parent_dir, 'table_statistics.json')
    with open(statistics_file, 'w') as json_file:
        json.dump(table_statistics, json_file, indent=2)
