import os
import pandas as pd
import streamlit as st
import json
import plotly.express as px
import yaml

def load_yaml_file(file_path):
    with open(file_path, 'r') as yaml_file:
        data = yaml.safe_load(yaml_file)
    return data

def load_json_file(file_path):

    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    return data

def create_datalake_dataframe(datalake_stats):
    rows = []
    for source, levels in datalake_stats.items():
        row = {'source': source}
        for level, stats in levels.items():
            for key, value in stats.items():
                row[f'{level}_{key}'] = value
        rows.append(row)
    return pd.DataFrame(rows)

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Construct absolute file paths for JSON files in the parent directory
datalake_stats_file = os.path.join(parent_dir, 'datalake_statistics.json')
database_stats_file = os.path.join(parent_dir, 'table_statistics.json')
execution_stats_file = os.path.join(parent_dir, 'execution_statistics.json')
yaml_file = os.path.join(parent_dir, 'project_walknet','sources.yaml')

# Load the JSON files
datalake_stats = load_json_file(datalake_stats_file)
database_stats = load_json_file(database_stats_file)
execution_stats = load_json_file(execution_stats_file)
yaml_data = load_yaml_file(yaml_file)


def plot_datalake_statistics():
    df = create_datalake_dataframe(datalake_stats)
    df.sort_values(by='source',inplace=True)

    # Create two separate stacked bar plots
    fig_data_size = px.bar(
        df, y='source', x=['level0_size_mb', 'level1_size_mb', 'level2_size_mb'],
        labels={'source': 'Source', 'value': 'Data Size (MB)'},
        barmode='stack',  # To show the bars stacked on top of each other
        orientation='h',  # Horizontal bars
        title='Datalake Statistics - Data Size (MB) by Source and Level (Stacked)'
    )

    fig_files = px.bar(
        df, y='source', x=['level0_total_files', 'level1_total_files', 'level2_total_files'],
        labels={'source': 'Source', 'value': 'Number of Files'},
        barmode='stack',  # To show the bars stacked on top of each other
        orientation='h',  # Horizontal bars
        title='Datalake Statistics - Number of Files by Source and Level (Stacked)'
    )

    # Update the layout to display the plots side by side
    fig_data_size.update_layout(
        legend_title='Level',  # Legend title
    )

    fig_files.update_layout(
        legend_title='Level',  # Legend title
    )

    st.plotly_chart(fig_data_size)
    st.plotly_chart(fig_files)


def plot_database_statistics():
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame.from_dict(database_stats, orient='index')
    df.sort_values('data_size_mb', inplace=True)

    fig_data_size = px.bar(
        df,
        y=df.index,
        x='data_size_mb',
        labels={'index': 'Table', 'data_size_mb': 'Data Size (MB)'},
        orientation='h',  # Horizontal bars
        title='Database Statistics - Data Size by Table'
    )

    st.plotly_chart(fig_data_size)

def plot_execution_statistics():
    # Convert the nested dictionary to a DataFrame
    rows = []
    for source, actions in execution_stats.items():
        for action, time in actions.items():
            rows.append({'source': source, 'action': action, 'execution_time': time})
    df = pd.DataFrame(rows)
    df.sort_values('source', inplace=True)

    fig_execution_time = px.bar(
        df,
        y='source',
        x='execution_time',
        color='action',
        labels={'source': 'Source', 'execution_time': 'Execution Time (s)', 'action': 'Action'},
        orientation='h',  # Horizontal bars
        title='Execution Statistics - Execution Time by Source and Action'
    )

    fig_execution_time.update_layout(
        barmode='stack'  # Stack bars for different actions
    )

    st.plotly_chart(fig_execution_time)


def main():
    st.title("Data Statistics Visualization")
    yaml_keys = list(yaml_data.keys())
    selected_key = st.selectbox('Select a key', yaml_keys)
    st.header("Execution Source Configurations")
    st.write(yaml_data[selected_key])
    # Check if the JSON data was loaded successfully
    if datalake_stats is None or database_stats is None or execution_stats is None:
        st.error("One or more JSON files not found. Make sure the files exist and provide the correct paths.")
        return
    
    st.header("Execution Statistics")
    plot_execution_statistics()
    
    st.header("Datalake Statistics")
    plot_datalake_statistics()

    st.header("Database Statistics")
    plot_database_statistics()

if __name__ == "__main__":
    main()
