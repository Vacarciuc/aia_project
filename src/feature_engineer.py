from pandas import DataFrame
import pandas as pd
import numpy as np


class FeatureEngineer:
    def __init__(self, data: DataFrame):
        self.data = data

    def execute(self) -> DataFrame:
        self._make_numeric_date_time()
        self._crate_season()
        self._crate_angle_of_sun()
        return self.data

    # drop date time by column
    def _make_numeric_date_time(self):
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['year'] = self.data['date'].dt.year
        self.data['month'] = self.data['date'].dt.month
        self.data['day'] = self.data['date'].dt.day

        self.data['hour'] = self.data['hour'].astype(str).str.slice(0,2).astype(int)

        #drop old column
        self.data = self.data.drop(columns=['date'])

    def _crate_season(self):
        self.data['season'] = np.where(self.data['month'].isin([12, 1, 2]), 1,
                              np.where(self.data['month'].isin([3, 4, 5]), 2,
                              np.where(self.data['month'].isin([6, 7, 8]), 3,
                                4)))
        # 1 - winter, 2 - spring, 3 - summer, 4 - autumn


    def _crate_angle_of_sun(self):
        #if is day = 0 if is night = 180
        # 90 is zenith
        # init with nan
        self.data['angle_of_sun'] = np.nan #todo solve, nan is not good for model

        for day, group in self.data.groupby('day'):
            day_indexes = group[group['is_day'] == 1].index

            if len(day_indexes) < 2:
                continue  # abnormal day, skip

            step = 180 / (len(day_indexes) - 1)

            for i, idx in enumerate(day_indexes):
                self.data.loc[idx, 'angle_of_sun'] = i * step



