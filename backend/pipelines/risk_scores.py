import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta

def compute_risk_scores():
    db_path = "data/processed/saferoute.sqlite"

    lgas = gpd.read_file(db_path, layer="lga_boundaries")
    incidents = gpd.read_file(db_path, layer="incidents")
    incidents['event_date'] = pd.to_datetime(incidents['event_date'])
    
    latest_date = incidents['event_date'].max()
    t30 = latest_date - timedelta(days=30)
    t90 = latest_date - timedelta(days=90)

    
    recent_incidents = incidents[incidents['event_date'] >= t30]
    historical_incidents = incidents[incidents['event_date'] >= t90]


    stats_30 = recent_incidents.groupby(['state', 'lga']).agg(
        incidents_30d=('event_date', 'count'),
        fatalities_30d=('fatalities', 'sum')
    ).reset_index()

    
    stats_90 = historical_incidents.groupby(['state', 'lga']).agg(
        incidents_90d=('event_date', 'count'),
        fatalities_90d=('fatalities', 'sum')
    ).reset_index()

    
    risk_df = lgas.merge(stats_30, on=['state', 'lga'], how='left')
    risk_df = risk_df.merge(stats_90, on=['state', 'lga'], how='left')
    
    
    risk_df = risk_df.fillna(0)

    
    risk_df['risk_score'] = (
        (risk_df['incidents_30d'] * 40) + 
        (risk_df['fatalities_30d'] * 30) +
        (risk_df['incidents_90d'] * 20) +
        (risk_df['fatalities_90d'] * 10)
    )

    
    max_score = risk_df['risk_score'].max()
    if max_score > 0:
        risk_df['risk_score'] = (risk_df['risk_score'] / max_score) * 100

    risk_df.to_file(db_path, layer='lga_risk_scores', driver='SQLite', spatialite=True)
    
    print("Risk scores computed and saved.")
    print(risk_df[['state', 'lga', 'risk_score']].sort_values('risk_score', ascending=False).head(10))

if __name__ == "__main__":
    compute_risk_scores()