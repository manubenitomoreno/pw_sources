import pandas as pd
from shapely import wkt
import geopandas as gpd


if __name__ == "__main__":
    edges = pd.read_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\edges.csv",sep=";")
    nodes = pd.read_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\networks\alcala_network\nodes.csv",sep=";")
    edges['geometry'] = edges['geometry'].apply(wkt.loads)
    nodes['netpointg'] = nodes['netpointg'].apply(wkt.loads)
    edges = gpd.GeoDataFrame(edges,geometry='geometry')
    edges['node1'] = 0
    edges['node2'] = 0
    nodes = gpd.GeoDataFrame(nodes,geometry='netpointg')
    nodes['coords'] = nodes.apply(lambda row: (row['netpointg'].x,row['netpointg'].y,row['netpointg'].z),axis=1)

    for i,row in edges.iterrows():
        #print(f"nodes for edge {row['id']}")
        l = list(row['geometry'].coords)
        ends = [l[0],l[-1]]
        print(ends,"--")
        edges.at[i,'node1'] = nodes[nodes['coords'] == ends[0]]['netpoint'].values[0]
        edges.at[i,'node2'] = nodes[nodes['coords'] == ends[1]]['netpoint'].values[0]
        
    print(edges)
    print(nodes)
    
    