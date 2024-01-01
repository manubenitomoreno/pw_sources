import pylandstats as pls
import pandas as pd
import numpy as np

def calculate_IJI(dataframe):
    # Sum the edge lengths for each unique pair
    edge_sums = dataframe.groupby(['origin', 'destination'])['edge_length'].sum().reset_index()

    # Calculate the total length of all edges
    total_edge_length = edge_sums['edge_length'].sum()

    # Calculate the proportion of the total edge length that each pair represents
    edge_sums['proportion'] = edge_sums['edge_length'] / total_edge_length

    # Calculate IJI
    edge_sums['iji_component'] = -edge_sums['proportion'] * np.log(edge_sums['proportion'])
    IJI_raw = edge_sums['iji_component'].sum()

    # Normalize IJI to a scale of 0 to 100
    max_IJI = -np.log(1/len(edge_sums))
    IJI_normalized = (IJI_raw / max_IJI) * 100

    return IJI_normalized

# Example usage:
# Assuming df is your DataFrame with columns 'origin', 'destination', and 'edge_length'
 IJI_value = calculate_IJI(df)
