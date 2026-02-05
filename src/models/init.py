import pandas as pd
from sklearn.model_selection import train_test_split
import os

print('Reading data...')
df = pd.read_excel(os.path.join('..', '..', 'cached_data', 'analyzed_data', 'weather_data.xlsx'))

TARGET = 'Produced Energy (kWh)'

FEATURES = [
   'global_tilted_irradiance',
   'temperature_2m',
   'wind_speed_10m',
   'Installed Power (kWp)',
   # 'hour'
]

RANDOM_STATE = 1
TEST_SIZE = 0.2
K_FOLD_SPLITS = 5

print('Preparing data...')
data = df[FEATURES + [TARGET]].dropna()

X = data[FEATURES]
y = data[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
   X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
)
