import pandas as pd
from pandas import DataFrame
from os import path

from src.save_data import SaveData, DataType


class PowerPLantService:
    def __init__(self, serial_nuber):
        self.serial_nuber = serial_nuber

    def exec(self):
        metadata = self._read_meta_data(self.serial_nuber)
        data_set = self._read_data_set_by_serial(self.serial_nuber)
        if metadata.empty:
            raise ValueError(f"No metadata found for serial {self.serial_nuber}")
        metadata_dict = metadata.iloc[0].to_dict()
        for col, value in metadata_dict.items():
            data_set[col] = value
        self._save_data(data_set)


    def _read_meta_data(self, serial_number:int):
        dirname = path.dirname(__file__)
        path_metadata = path.join(dirname, '..', 'cached_data', 'power_plant', 'PV Plants Metadata.xlsx')
        path_metadata = path.normpath(path_metadata)
        data = pd.read_excel(path_metadata, header=1)
        result = data[data["PV Serial Number"] == serial_number]
        return result

    def _read_data_set_by_serial(self, serial_number:int):
        dirname = path.dirname(__file__)
        path_data_set = path.join(dirname, '..', 'cached_data', 'power_plant', 'PV Plants Datasets.xlsx')
        path_data_set = path.normpath(path_data_set)

        data = pd.read_excel(path_data_set, sheet_name=str(serial_number))
        return data

    def _save_data(self, df):
        save_obj = SaveData(file_name='power-plant', data_type=DataType.PowerPlant)
        save_obj.save(df)

    # def _clean_and_save_data(self, data: DataFrame):
    #     data_cleaner = DataCleaner(data)
    #     clean_data = data_cleaner.clean()
    #     clean_data["datetime"] = pd.to_datetime(clean_data["Date"], unit="ns")
    #
    #     clean_data["year"] = clean_data["datetime"].dt.year
    #     clean_data["month"] = clean_data["datetime"].dt.month
    #     clean_data["day"] = clean_data["datetime"].dt.day
    #     clean_data["hour"] = clean_data["datetime"].dt.hour
    #
    #     clean_data.drop(columns=["Date", "datetime"], inplace=True)
    #
    #     save_obj = SaveData(file_name='power-plant', data_type=DataType.PowerPlant)
    #     save_obj.save(clean_data)
    #
    # def _join_and_insert_general_file(self):
    #     weather_df = pd.read_excel(r"D:\PROJECT\Python\aia_project\cached_data\analyzed_data\weather_data.xlsx")
    #     energy_df = pd.read_excel(r"D:\PROJECT\Python\aia_project\cached_data\power_plant\power-plant.xlsx")
    #
    #     analysis_df = pd.merge(
    #         weather_df,
    #         energy_df,
    #         on=['year', 'month', 'day', 'hour'],
    #         how='inner'
    #     )
    #
    #     analysis_df.to_excel(
    #         r"D:\PROJECT\Python\aia_project\cached_data\analyze_data\weather_data.xlsx",
    #         index=False
    #     )

