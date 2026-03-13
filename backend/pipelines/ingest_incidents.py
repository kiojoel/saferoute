import pandas as pd
import geopandas as gpd
import os

def ingest_incidents():
    db_path = "data/processed/saferoute.sqlite"
    csv_path = "data/raw/nigeria_incidents.csv"

    df = pd.read_csv(csv_path)

    gdf_incidents = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    lga_boundaries = gpd.read_file(db_path, layer="lga_boundaries")
    
    joined_gdf = gpd.sjoin(gdf_incidents, lga_boundaries, how="left", predicate="intersects")

    joined_gdf['event_date'] = pd.to_datetime(joined_gdf['date_start'])

    final_gdf = joined_gdf[[
        'event_date', 'type_of_violence', 'best', 'deaths_civilians', 
        'state', 'lga', 'latitude', 'longitude', 'geometry'
    ]].copy()

    final_gdf.columns = [
        'event_date', 'event_type', 'fatalities', 'civilian_fatalities',
        'state', 'lga', 'latitude', 'longitude', 'geometry'
    ]

    final_gdf.to_file(db_path, layer='incidents', driver='SQLite', spatialite=True)
    print(f"Successfully ingested {len(final_gdf)} incidents into the 'incidents' layer.")

if __name__ == "__main__":
    ingest_incidents()