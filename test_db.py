import geopandas as gpd
import pandas as pd

# gdf = gpd.read_file('data/processed/saferoute.sqlite', layer='lga_boundaries')

# print(f"Number of rows: {len(gdf)}")
# print()

# print("Column names:")
# print(gdf.columns.tolist())
# print()

# print("First 3 rows (state, lga, geometry):")
# print(gdf[['state', 'lga', 'geometry']].head(3))
# print()

# print(f"Number of unique states: {gdf['state'].nunique()}")
# print("Unique states:")
# print(gdf['state'].unique())

db_path = "data/processed/saferoute.sqlite"
incidents_gdf = gpd.read_file(db_path, layer='incidents')

# Print row count
print(f"Number of rows: {len(incidents_gdf)}")
print()

# Print column names
print("Column names:")
print(incidents_gdf.columns.tolist())
print()

# Print the first 3 rows
print("First 3 rows:")
print(incidents_gdf.head(3))
print()

# Print the value counts of violence_type
print("Violence type counts:")
print(incidents_gdf['violence_type'].value_counts())
print()

# Print how many incidents have a null lga
null_lga_count = incidents_gdf['lga'].isna().sum()
print(f"Number of incidents with null LGA: {null_lga_count}")
print(f"Percentage of incidents with null LGA: {(null_lga_count/len(incidents_gdf))*100:.2f}%")