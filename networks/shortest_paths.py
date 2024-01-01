import networkx as nx
import pandas as pd
import json
import numpy as np


def json_data(row, columns):
    """ 
    Just a simple function to convert a list of columns into a JSON column
    """
    data = {col: row[col] for col in columns if col in row.index}
    return json.dumps(data)

#edges = pd.read_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\edges.csv", sep =";")
def make_shortest_paths(edges: pd.DataFrame):
    #edges['data'] = edges.apply(lambda x: json.loads(x['data']), axis = 1)
    edges['length'] = np.ceil(edges['data'].str['length'])

    G = nx.from_pandas_edgelist(edges, "start", "end", edge_attr = ["length"]).to_undirected()

    length = {k: {ke: va for ke, va in v.items() if ke !=k } for k,v in dict(nx.all_pairs_dijkstra_path_length(G, cutoff = 900, weight='length')).items()}
    path_nodes = {k: {ke: va for ke, va in v.items() if ke !=k } for k,v in dict(nx.all_pairs_dijkstra_path(G, cutoff = 900, weight='length')).items()}
    ego_graphs = {node: {cut_value :[k for k in nx.single_source_dijkstra_path_length(G, node, cutoff= cut_value, weight='length').keys() if k != node] for cut_value in [300,600,900]} for node in list(G.nodes())}
    shortest_paths = pd.concat([pd.Series(path_nodes),pd.Series(length),pd.Series(ego_graphs)],axis = 1).reset_index(drop=False).rename(columns={'index':'node_id',0:'path_nodes',1:'length',2:'ego_graphs'})
    to_concat=[]
    for metric,alias in {'path_nodes':'spn','length':'spl','ego_graphs':'ego'}.items():
        df = shortest_paths.copy()
        df['relation_id'] = df.apply(lambda x: f"{alias}|{x['node_id']}",axis=1)
        df['relation_kind'] = metric
        df['data'] = df[metric]
        df = df[['relation_id','relation_kind','data']]
        to_concat.append(df)
    df = pd.concat(to_concat)
    df['data'] = df.apply(lambda x: json.dumps(x['data']),axis=1)
    #df['data']
    return df