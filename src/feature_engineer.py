from pandas import DataFrame
import pandas as pd
import numpy as np


class FeatureEngineer:
    def __init__(self, data: DataFrame):
        self.data = data.copy()

    def execute(self) -> DataFrame:
        self._prepare_datetime()
        self._add_time_features()
        self._add_wind_features()
        self._add_cloud_features()
        self._add_thermal_features()
        self._cleanup_columns()
        return self.data

    def _prepare_datetime(self):
        self.data['date'] = pd.to_datetime(self.data['date'])

    def _add_time_features(self):
        self.data['month'] = self.data['date'].dt.month
        self.data['hour'] = self.data['date'].dt.hour

        self.data['month_sin'] = np.sin(2 * np.pi * self.data['month'] / 12)
        self.data['month_cos'] = np.cos(2 * np.pi * self.data['month'] / 12)

        self.data['hour_sin'] = np.sin(2 * np.pi * self.data['hour'] / 24)
        self.data['hour_cos'] = np.cos(2 * np.pi * self.data['hour'] / 24)

    def _add_wind_features(self):
        direction_rad = np.deg2rad(self.data['wind_direction_10m'])

        self.data['wind_u'] = self.data['wind_speed_10m'] * np.cos(direction_rad)
        self.data['wind_v'] = self.data['wind_speed_10m'] * np.sin(direction_rad)

    def _add_cloud_features(self):
        self.data['cloud_verticality'] = (
            self.data['cloud_cover_high'] - self.data['cloud_cover_low']
        )

    def _add_thermal_features(self):
        self.data['heat_moisture_index'] = (
            self.data['temperature_2m'] *
            (1 - self.data['relative_humidity_2m'] / 100)
        )

    def _cleanup_columns(self):
        drop_cols = [
            'apparent_temperature',
            'dew_point_2m',
            'rain',
            'pressure_msl',
            'month',
            'hour',
            'wind_direction_10m',
            'cloud_cover_low',
            'cloud_cover_mid',
            'cloud_cover_high'
        ]

        self.data.drop(
            columns=[c for c in drop_cols if c in self.data.columns],
            inplace=True
        )
