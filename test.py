import pandas as pd
df = pd.read_csv("data/raw/nigeria_incidents.csv")

print(df.shape)
print(df['best'].isna().sum())
print(df[df['best'].isna()][['date_start', 'type_of_violence', 'best']].head())