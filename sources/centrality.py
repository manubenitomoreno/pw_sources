import os
import processing
from qgis.core import QgsProject, QgsVectorLayer

# Create a temporary directory if it doesn't exist
output_dir = r'C:/temp'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def delete_if_exists(filepath):
    """Delete the file if it exists."""
    if os.path.exists(filepath):
        os.remove(filepath)
        print(f"Deleted existing file: {filepath}")

# Load the network layer and the polygon layer
print("Loading network and polygon layers...")
network_layer = QgsVectorLayer(r'G:\Mi unidad\walknet_datalake\sources\amm_network\level1\level1_amm_network_000.gpkg', 'Network Layer', 'ogr')
polygon_layer = QgsVectorLayer(r'G:\Mi unidad\walknet_datalake\sources\ine_geo\level1\level1_ine_geo_2018.gpkg', 'Polygon Layer', 'ogr')

# Check if layers loaded correctly
if not network_layer.isValid() or not polygon_layer.isValid():
    print("Error loading layers")
    exit()

print("Layers loaded successfully.")

# Add layers to QGIS if not already loaded
QgsProject.instance().addMapLayer(network_layer)
QgsProject.instance().addMapLayer(polygon_layer)

# Step 1: Apply filter to the polygon layer
print("Applying filter to polygon layer...")
polygon_filter = ('"boundary_type" = \'municipality\' '
                  'AND "boundary_code" IN (\'28005\', \'28006\', \'28007\', \'28022\', \'28026\', \'28045\', \'28049\', '
                  '\'28058\', \'28065\', \'28073\', \'28074\', \'28080\', \'28084\', \'28092\', \'28104\', '
                  '\'28106\', \'28113\', \'28115\', \'28127\', \'28130\', \'28134\', \'28148\', \'28167\', '
                  '\'28176\', \'28177\', \'28181\', \'28903\', \'28079\', \'28123\', \'28903\')')
polygon_layer.setSubsetString(polygon_filter)

print("Filter applied.")

# Step 2: Split the network using the filtered polygon layer
print("Splitting the network based on polygons...")
split_output = processing.run("native:splitwithlines", {
    'INPUT': network_layer,
    'LINES': polygon_layer,
    'OUTPUT': os.path.join(output_dir, 'split_network.gpkg')  # Save to disk
})

split_layer = QgsVectorLayer(os.path.join(output_dir, 'split_network.gpkg'), 'Split Network', 'ogr')
print("Network split completed.")

# Step 3: Loop through each part of the split network and compute betweenness centrality
print("Starting centrality calculation for each boundary code...")

# Get the list of unique boundary codes from the filtered polygon layer
unique_boundary_codes = polygon_layer.uniqueValues(polygon_layer.fields().lookupField('boundary_code'))

for boundary_code in unique_boundary_codes:
    print(f"Processing boundary code: {boundary_code}")
    
    # Filter the split layer based on the current boundary code
    subset_expression = f'"boundary_code" = {boundary_code}'
    split_layer.setSubsetString(subset_expression)
    
    # Export the subset of the network to a temporary GeoPackage file in the local directory
    temp_network_file = os.path.join(output_dir, f'network_subset_{boundary_code}.gpkg')
    
    # Check if file exists and delete if needed
    delete_if_exists(temp_network_file)

    QgsVectorFileWriter.writeAsVectorFormat(split_layer, temp_network_file, 'utf-8', driverName='GPKG')

    # Check if the file was created
    if not os.path.exists(temp_network_file):
        print(f"Error: {temp_network_file} was not created.")
        continue

    print(f"Calculating betweenness centrality for boundary code: {boundary_code}")

    # Step 4: Run GRASS v.net.centrality on the split network
    grass_output = processing.run("grass7:v.net.centrality", {
        'input': temp_network_file,
        'centrality': 'betweenness',  # Compute betweenness centrality
        'output': os.path.join(output_dir, f'centrality_output_{boundary_code}.gpkg')
    })

    print(f"Centrality calculation completed for boundary code: {boundary_code}")

    # Load the centrality results into QGIS
    centrality_layer = QgsVectorLayer(grass_output['output'], f'Centrality_{boundary_code}', 'ogr')
    QgsProject.instance().addMapLayer(centrality_layer)

print("All boundary codes processed.")

# Step 5: Optionally merge all centrality layers together
print("Merging centrality layers...")
merged_output_file = r'G:\Mi unidad\walknet_datalake\sources\centrality\level2\level1_amm_network_centrality.gpkg'

# Check if the merged file exists and delete if needed
delete_if_exists(merged_output_file)

merge_output = processing.run("native:mergevectorlayers", {
    'LAYERS': [os.path.join(output_dir, f'centrality_output_{code}.gpkg') for code in unique_boundary_codes],
    'OUTPUT': merged_output_file
})

print("Centrality layers merged.")

# Add the merged result
merged_layer = QgsVectorLayer(merged_output_file, 'Merged Centrality', 'ogr')
QgsProject.instance().addMapLayer(merged_layer)

print("Betweenness centrality calculation completed successfully!")
