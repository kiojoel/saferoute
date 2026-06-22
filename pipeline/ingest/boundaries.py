import os
import sys
import geopandas as gpd
import psycopg2
from dotenv import load_dotenv, find_dotenv
from shapely.geometry import MultiPolygon

dotenv_path = find_dotenv()
print(f"Loading .env from: {dotenv_path}")
load_dotenv(dotenv_path)

gpkg_path = os.getenv("GADM_PATH")
if not gpkg_path:
    print("ERROR: GADM_PATH not set in .env")
    sys.exit(1)

db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")

if not all([db_name, db_user, db_pass]):
    print("ERROR: Missing database credentials in .env")
    sys.exit(1)

print(f"Reading LGA boundaries from: {gpkg_path}")
gdf = gpd.read_file(gpkg_path, layer="ADM_ADM_2", engine="pyogrio")

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_pass
)
cur = conn.cursor()


cur.execute("SELECT id FROM countries WHERE iso_a3 = 'NGA'")
row = cur.fetchone()
if not row:
    print("ERROR: Nigeria not found in countries table.")
    sys.exit(1)
country_id = row[0]
print(f"Nigeria country_id = {country_id}")


inserted = 0
for idx, lga in gdf.iterrows():
    code = lga["GID_2"]
    name = lga["NAME_2"]
    parent_code = lga["GID_1"]
    geom = lga["geometry"]

    # Ensure geometry is MultiPolygon (schema requirement)
    if geom.geom_type == "Polygon":
        geom = MultiPolygon([geom])

    # Convert to Well‑Known Text (WKT)
    wkt_geom = geom.wkt

    
    cur.execute("""
        INSERT INTO admin_units (country_id, code, name, level, parent_code, geometry)
        VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        ON CONFLICT (code) DO UPDATE
        SET name = EXCLUDED.name, geometry = EXCLUDED.geometry
    """, (country_id, code, name, 2, parent_code, wkt_geom))

    inserted += 1
    if inserted % 100 == 0:
        print(f"Inserted {inserted} LGAs...")
        conn.commit()


conn.commit()
cur.close()
conn.close()
print(f"Successfully inserted/updated {inserted} LGAs.")