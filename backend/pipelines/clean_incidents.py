import pandas as pd

def clean_incidents():
    df = pd.read_csv("data/raw/nigeria_incidents.csv")
    df = df[["date_start", "type_of_violence", "best", "deaths_civilians", "latitude", "longitude", "country", "adm_1", "adm_2"]]

    df = df.rename(
        columns={
            "date_start": "event_date",
            "type_of_violence": "violence_type",
            "best": "fatalities",
            "deaths_civilians": "civilian_deaths",
            "adm_1": "state_raw",
            "adm_2": "lga_raw",
        }
    )

    violence_map = {
        1: "state_based",
        2: "non_state",
        3: "one_sided",
    }

    df["violence_type"] = df["violence_type"].map(violence_map)

    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df = df[df["event_date"] >= "2010-01-01"]
    df = df.dropna(subset=["event_date"])

    df.to_csv("data/processed/incidents_cleaned.csv", index=False)

    print("Rows remaining:", len(df))
    print("Date range:", df["event_date"].min(), "to", df["event_date"].max())
    print("\nViolence type counts:")
    print(df["violence_type"].value_counts())

    return df 

if __name__ == "__main__":
    clean_incidents()
