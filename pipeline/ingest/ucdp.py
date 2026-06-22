import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")

if not all([db_name, db_user, db_pass]):
    print("ERROR: Missing database credentials in .env")
    sys.exit(1)

ucdp_path = os.getenv("UCDP_PATH")
if not ucdp_path:
    print("ERROR: UCDP_PATH not set in .env")
    sys.exit(1)


print(f"Reading UCDP data from: {ucdp_path}")


df = pd.read_csv(ucdp_path, low_memory=False)


df_ng = df[df['country'] == 'Nigeria'].copy()
total_ng_events = len(df_ng)
print(f"Total Nigeria events: {total_ng_events}")

df_filtered = df_ng[~df_ng['where_prec'].isin([5, 6])].copy()
filtered_count = len(df_filtered)
print(f"Events after precision filter (excluded 5,6): {filtered_count}")
print(f"Excluded {total_ng_events - filtered_count} low-precision events")

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
    print("ERROR: Nigeria not found in countries table. Did you seed it?")
    sys.exit(1)
country_id = row[0]
print(f"Nigeria country_id = {country_id}")


violence_map = {
    1: 'state_based',
    2: 'non_state',
    3: 'one_sided'
}

inserted = 0
skipped = 0

for idx, event in df_filtered.iterrows():
    source_id = str(event['id'])
    event_date = pd.to_datetime(event['date_start']).date()
    event_type = violence_map.get(event['type_of_violence'], 'unknown')
    location_name = event['adm_2']
    if pd.isna(location_name) or location_name == 'NA':
        location_name = None
    latitude = event['latitude']
    longitude = event['longitude']
    fatalities = int(event['best']) if pd.notna(event['best']) else 0
    location_precision = int(event['where_prec']) if pd.notna(event['where_prec']) else None
    description = event['source_headline'] if pd.notna(event['source_headline']) else None

    wkt_point = f"POINT({longitude} {latitude})"

    try:
        cur.execute("""
            INSERT INTO conflict_events (
                source, source_id, country_id, event_date, event_type, location_name,
                latitude, longitude, geometry, fatalities, description,
                location_precision
            )
            VALUES (
                'ucdp', %s, %s, %s, %s, %s,
                %s, %s, ST_GeomFromText(%s, 4326),
                %s, %s, %s
            )
            ON CONFLICT (source, source_id) DO NOTHING
        """, (
            source_id, country_id, event_date, event_type, location_name,
            latitude, longitude, wkt_point, fatalities, description,
            location_precision
        ))

        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

        if (inserted + skipped) % 500 == 0:
            print(f"Processed {inserted + skipped} events...")
            conn.commit()

    except Exception as e:
        print(f"Error inserting event {source_id}: {e}")
        conn.rollback()

conn.commit()
print(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")


cur.execute("""
    UPDATE conflict_events ce
    SET admin_unit_id = au.id
    FROM admin_units au
    WHERE ST_Contains(au.geometry, ce.geometry)
      AND au.level = 2
      AND ce.admin_unit_id IS NULL
      AND ce.source = 'ucdp'
      AND ce.location_precision IS NOT NULL
      AND ce.location_precision <= 4
""")
matched = cur.rowcount
conn.commit()


cur.execute("""
    SELECT COUNT(*) FROM conflict_events
    WHERE source = 'ucdp' AND admin_unit_id IS NULL
""")
unmatched = cur.fetchone()[0]
print(f"Events without LGA assignment: {unmatched}")



print("UCDP Ingestion Summary")
print(f"Total Nigeria events in source:   {total_ng_events}")
print(f"Filtered out (precision 5,6):     {total_ng_events - filtered_count}")
print(f"Attempted to insert:               {filtered_count}")
print(f"Inserted (new):                    {inserted}")
print(f"Skipped (duplicates):              {skipped}")
print(f"Spatial join matched:              {matched}")
print(f"Unmatched (no LGA found):          {unmatched}")


cur.close()
conn.close()