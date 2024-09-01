import pandas as pd
import geopandas as gpd
import numpy as np
import networkx as nx
from typing import Dict, Set, Tuple
from shapely.wkt import loads

from shapely.strtree import STRtree
from collections import defaultdict

def create_graph_from_edges(in_edges: pd.DataFrame) -> nx.Graph:
    """Create an undirected graph from edge data."""
    return nx.from_pandas_edgelist(in_edges, "start", "end", edge_attr=["length"]).to_undirected()

def update_dangle_flags(in_edges: pd.DataFrame, ends: Set) -> None:
    """Update the dangle flags in the edges DataFrame."""
    ends_array = np.array(list(ends))
    in_edges['start_dangle'] = in_edges['start'].isin(ends_array).astype(int)
    in_edges['end_dangle'] = in_edges['end'].isin(ends_array).astype(int)


def find_culdesacs(G: nx.Graph, in_edges: pd.DataFrame) -> Dict:
    """Find and return culdesacs from the graph."""
    output = {}
    i = 1
    ends = {c[0] for c in G.degree if c[1] == 1}

    #while True:
    while ends:
        update_dangle_flags(in_edges, ends)
        #in_edges['culdesac'] = in_edges.apply(lambda row: i if row['start_dangle'] == 1 or row['end_dangle'] == 1 else 0, axis=1)
        in_edges['culdesac'] = np.where((in_edges['start_dangle'] == 1) | (in_edges['end_dangle'] == 1), i, 0) #GPT SUGGESTION
        #culdesacs = {k: v for k, v in pd.Series(in_edges.culdesac.values, index=in_edges.edge_id).to_dict().items() if v == i}
        culdesacs = in_edges.loc[in_edges['culdesac'] == i, 'edge_id'].to_dict()
        

        if not culdesacs:
            break

        output.update(culdesacs)
        #in_edges = in_edges[in_edges['culdesac'] != i]
                # Remove the edges and their nodes from the graph
        for edge in in_edges[in_edges['culdesac'] == i].itertuples():
            if G.has_edge(edge.start, edge.end):
                G.remove_edge(edge.start, edge.end)
            # Optionally remove nodes that have no remaining connections (degree 0)
            if G.degree[edge.start] == 0:
                G.remove_node(edge.start)
            if G.degree[edge.end] == 0:
                G.remove_node(edge.end)
        #G = create_graph_from_edges(in_edges)
        # GPT SUGGESTS Instead of recreating the graph, consider removing nodes directly
        ends = {c[0] for c in G.degree if c[1] == 1}
        i += 1

    return output


def contiguous_culdesacs(in_edges):

    #depth = list(in_edges['culdesac'].unique())
    #all_cds = {id:[id] for id in list(in_edges[in_edges['culdesac']==1]['edge_id'].unique())}
    tree = STRtree(in_edges['geometry'].apply(loads))
    all_cds = defaultdict(list)
    depth = sorted(in_edges['culdesac'].unique())

    for d in depth:
        filtered_edges = in_edges[in_edges['culdesac'] == d]
        if not filtered_edges.empty:
            for idx, row in filtered_edges.iterrows():
                geo = loads(row['geometry'])
                candidates = tree.query(geo)
                for candidate in candidates:
                    if geo.touches(candidate):
                        all_cds[row['edge_id']].append(candidate)

    """
    for d in depth:
        for id in all_cds.keys():
            last_id = all_cds[id][-1]
            filter_contiguous = in_edges[in_edges['culdesac']==d+1]
            if not filter_contiguous.empty:
                geo = loads(in_edges[in_edges['edge_id']==last_id]['geometry'].values[0])
                for i, row in filter_contiguous.iterrows():
                    candidate = loads(row['geometry'])
                    if candidate.touches(geo):
                        all_cds[id].append(row['edge_id'])
                    else:
                        pass
            else:
                pass
    """

    # Using 
    result = {}
    for k,v in all_cds.items():
        for val in v:
            result[val] = k
    
    return in_edges['edge_id'].map(result)
    
def culdesacs(in_edges):
    
    """Main function to process edges and return culdesacs."""
    print("Building length...")
    in_edges['length'] = np.ceil(in_edges['data'].str['length'])
    print("Creating Graph...")
    G = create_graph_from_edges(in_edges)
    print("Finding Culdesacs...")
    culdesac_output = find_culdesacs(G, in_edges)
    in_edges['culdesac'] = in_edges['edge_id'].map(culdesac_output)
    print("Finding Contiguous Culdesacs...")
    in_edges['culdesac'] = contiguous_culdesacs(in_edges)
    in_edges['geometry'] = in_edges['geometry'].apply(loads)
    print("Re-generate geometry...")
    length = gpd.GeoDataFrame(in_edges,geometry='geometry').dissolve(by='culdesac').reset_index(drop=False)
    length['length'] = length['geometry'].length
    in_edges['culdesac_length'] = in_edges['culdesac'].map(pd.Series(length['length'].values,index=length['culdesac']).to_dict())
    #print(in_edges)
    return in_edges

