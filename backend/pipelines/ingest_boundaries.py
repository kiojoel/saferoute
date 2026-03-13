import os
import geopandas as gpd

def ingest_to_spatialite():
    raw_path = "data/raw/gadm41_NGA.gpkg"
    db_path = "data/processed/saferoute.sqlite"

    
    os.makedirs("data/processed", exist_ok=True)

    layer_name = "ADM_ADM_2" 
    
    print(f"Reading GADM file layer: {layer_name}...")
    df = gpd.read_file(raw_path, layer=layer_name) 

    
    df_filtered = df[['NAME_1', 'NAME_2', 'geometry']].copy()
    df_filtered = df_filtered.rename(columns={
        'NAME_1': 'state',
        'NAME_2': 'lga'
    })

    print(f"Saving {len(df_filtered)} LGAs to SpatiaLite...")

    if os.path.exists(db_path):
        os.remove(db_path)
        
    df_filtered.to_file(
        db_path, 
        layer='lga_boundaries', 
        driver='SQLite', 
        spatialite=True
    )
    
    print(f"✅ Success! Database created at {db_path}")

if __name__ == "__main__":
    ingest_to_spatialite()