import pandas as pd
from shapely import wkt
import geopandas as gpd
import networkx as nx


if __name__ == "__main__":
    edges = pd.read_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\edges.csv",sep=";")
    nodes = pd.read_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\nodes.csv",sep=";")
    print(edges)
    print(nodes)
    
    G = nx.from_pandas_edgelist(edges, "node0_id", "node1_id", ["edge_id", "length"]).to_undirected()
    print(G)
    n_connected = nx.number_connected_components(G)
    print(f"There are {n_connected} connected components in the graph.")
    largest_cc = max(nx.connected_components(G), key=len)
    subgraph = G.subgraph(largest_cc).copy()
    centr = nx.edge_current_flow_betweenness_centrality(subgraph, normalized=True, weight='length', solver='cg')
    
    # Continue after calculating centrality...
    edge_centrality_list = []

    for (u, v, attr) in subgraph.edges(data=True):
        edge_id = attr['edge_id']
        centr_value = centr[(u, v)] if (u, v) in centr else centr[(v, u)]
        edge_centrality_list.append({
            'edge_id': edge_id,
            'centrality': centr_value
        })

    # Convert to DataFrame
    edge_centrality_df = pd.DataFrame(edge_centrality_list)

    # Merge with original edges dataframe
    merged_df = pd.merge(edges, edge_centrality_df, on='edge_id', how='left')

    merged_df.to_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\centrality.csv",sep=";",index=False)

    

