import pandas as pd
import geopandas as gpd
import numpy as np
import networkx as nx
from typing import Dict, Set, Tuple
from shapely.wkt import loads

def create_graph_from_edges(in_edges: pd.DataFrame) -> nx.Graph:
    """Create an undirected graph from edge data."""
    return nx.from_pandas_edgelist(in_edges, "start", "end", edge_attr=["length"]).to_undirected()

def update_dangle_flags(in_edges: pd.DataFrame, ends: Set) -> None:
    """Update the dangle flags in the edges DataFrame."""
    in_edges['start_dangle'] = in_edges['start'].isin(ends).astype(int)
    in_edges['end_dangle'] = in_edges['end'].isin(ends).astype(int)

def find_culdesacs(G: nx.Graph, in_edges: pd.DataFrame) -> Dict:
    """Find and return culdesacs from the graph."""
    output = {}
    i = 1
    ends = {c[0] for c in G.degree if c[1] == 1}

    while True:
        update_dangle_flags(in_edges, ends)
        in_edges['culdesac'] = in_edges.apply(lambda row: i if row['start_dangle'] == 1 or row['end_dangle'] == 1 else 0, axis=1)
        culdesacs = {k: v for k, v in pd.Series(in_edges.culdesac.values, index=in_edges.edge_id).to_dict().items() if v == i}

        if not culdesacs:
            break

        output.update(culdesacs)
        in_edges = in_edges[in_edges['culdesac'] != i]
        G = create_graph_from_edges(in_edges)
        ends = {c[0] for c in G.degree if c[1] == 1}
        i += 1

    return output


def contiguous_culdesacs(in_edges):
    depth = list(in_edges['culdesac'].unique())
    all_cds = {id:[id] for id in list(in_edges[in_edges['culdesac']==1]['edge_id'].unique())}
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
        
    # Using 
    result = {}
    for k,v in all_cds.items():
        for val in v:
            result[val] = k
    
    return in_edges['edge_id'].map(result)
    
def culdesacs(in_edges):
    
    """Main function to process edges and return culdesacs."""
    in_edges['length'] = np.ceil(in_edges['data'].str['length'])
    G = create_graph_from_edges(in_edges)
    culdesac_output = find_culdesacs(G, in_edges)
    in_edges['culdesac'] = in_edges['edge_id'].map(culdesac_output)
    in_edges['culdesac'] = contiguous_culdesacs(in_edges)
    in_edges['geometry'] = in_edges['geometry'].apply(loads)
    length = gpd.GeoDataFrame(in_edges,geometry='geometry').dissolve(by='culdesac').reset_index(drop=False)
    length['length'] = length['geometry'].length
    in_edges['culdesac_length'] = in_edges['culdesac'].map(pd.Series(length['length'].values,index=length['culdesac']).to_dict())
    #print(in_edges)
    return in_edges

