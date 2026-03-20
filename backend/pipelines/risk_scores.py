import pandas as pd
import geopandas as gpd
from datetime import timedelta

def compute_risk_scores():
    db_path = "data/processed/saferoute.sqlite"

    lgas = gpd.read_file(db_path, layer="lga_boundaries")
    incidents = gpd.read_file(db_path, layer="incidents")
    incidents['event_date'] = pd.to_datetime(incidents['event_date'])

    latest_date = incidents['event_date'].max()
    t30 = latest_date - timedelta(days=30)   # cutoff for last 30 days

    recent = incidents[incidents['event_date'] >= t30]

    stats_30 = recent.groupby(['state', 'lga']).agg(
        incident_count=('event_date', 'count'),
        fatalities_total=('fatalities', 'sum'),
        civilian_deaths_total=('civilian_deaths', 'sum')
    ).reset_index()

    stats_30['severity'] = (stats_30['civilian_deaths_total'] * 1.5) + stats_30['fatalities_total']

    risk_df = lgas.merge(stats_30, on=['state', 'lga'], how='left')
    risk_df = risk_df.fillna(0)   # LGAs with no incidents get zeros

    last_incident = incidents.groupby(['state', 'lga'])['event_date'].max().reset_index()
    last_incident.rename(columns={'event_date': 'last_incident'}, inplace=True)

    risk_df = risk_df.merge(last_incident, on=['state', 'lga'], how='left')
    risk_df['last_incident'] = risk_df['last_incident'].fillna(pd.Timestamp('1900-01-01'))

    days_since = (latest_date - risk_df['last_incident']).dt.days
    risk_df['recency_factor'] = (1 - days_since / 365).clip(lower=0)

    risk_df.loc[risk_df['incident_count'] == 0, 'recency_factor'] = 0

    max_incident = risk_df['incident_count'].max()
    max_fatalities = risk_df['fatalities_total'].max()
    max_severity = risk_df['severity'].max()

    if max_incident > 0:
        risk_df['incident_norm'] = risk_df['incident_count'] / max_incident
    else:
        risk_df['incident_norm'] = 0

    if max_fatalities > 0:
        risk_df['fatalities_norm'] = risk_df['fatalities_total'] / max_fatalities
    else:
        risk_df['fatalities_norm'] = 0

    if max_severity > 0:
        risk_df['severity_norm'] = risk_df['severity'] / max_severity
    else:
        risk_df['severity_norm'] = 0

    # (weights sum to 1.0)
    risk_df['raw_score'] = (risk_df['incident_norm'] * 0.40) + \
                           (risk_df['fatalities_norm'] * 0.30) + \
                           (risk_df['severity_norm'] * 0.20) + \
                           (risk_df['recency_factor'] * 0.10)

    # Scale to 0–100
    risk_df['risk_score'] = risk_df['raw_score'] * 100

    risk_df.to_file(db_path, layer='lga_risk_scores', driver='SQLite', spatialite=True)

    print("Risk scores computed and saved.\n")
    print("Top 10 LGAs by risk score:")
    top10 = risk_df[['state', 'lga', 'risk_score']].sort_values('risk_score', ascending=False).head(10)
    print(top10.to_string(index=False))

    zero_count = (risk_df['risk_score'] == 0).sum()
    print(f"\nNumber of LGAs with a risk score of 0: {zero_count}")

if __name__ == "__main__":
    compute_risk_scores()