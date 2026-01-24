# Display correlations between features and target variable sorted in descending order

import pandas as pd

df = pd.read_excel('../../cached_data/analyzed_data/weather_data.xlsx', na_values=[''])
df = df.dropna()

print(df.corr()['Produced Energy (kWh)'].dropna().sort_values(ascending=False))
