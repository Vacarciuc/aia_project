from dataclasses import dataclass
from os import path
from enum import Enum
import sys

import pandas as pd
from numpy.ma.core import power
from pandas import DataFrame
from src.api_request import ApiRequest
from src.openmeteo_parser import OpenMeteoParser
from src.data_cleaner import DataCleaner
from src.save_data import SaveData, DataType
from src.data_analysis import Analysis
from src.feature_engineer import FeatureEngineer
from src.power_plant_service import PowerPLantService


class CommandEnum(Enum):
    API_REQUEST = "api_request",
    SAVE_CLEAN_DATA = "save_clean_data",
    ANALYZE_DATA = "analyze_data",
    POWER_PLANT = "power_plant",
    JOIN_DATA = "join_data",


@dataclass
class RequestParams:
    latitude: float = 0.0
    longitude: float = 0.0
    start_date: str = ""
    end_date: str = ""

class Command:
    def __init__(self):
        self.request_params = RequestParams()

    def execute(self, command: CommandEnum, serial_number:int) -> DataFrame | None:
        request_params = RequestParams()
        file_name = 'weather_data.xlsx'
        if command == CommandEnum.POWER_PLANT:
            data = self._read_power_plant_file()
            if data is None or data.empty:
                print(f"No data found for serial {serial_number}")
                return None

            data.columns = data.columns.str.strip().str.lower()
            row = data.iloc[1]

            from_date = pd.to_datetime(row.get('from date')).strftime('%Y-%m-%d')
            to_date = pd.to_datetime(row.get('to date')).strftime('%Y-%m-%d')

            self.request_params.latitude = float(row.get('latitude'))
            self.request_params.longitude = float(row.get('longitude'))
            self.request_params.start_date = from_date
            self.request_params.end_date = to_date
            return data

        elif command == CommandEnum.API_REQUEST:
            print('request parameters:', self.request_params)
            self._api_request(self.request_params)
            return self._read_file(DataType.DirtyData, file_name)

        elif command == CommandEnum.JOIN_DATA:
            self._join_data()
            return self._read_file(DataType.JoinData, 'weather-power.xlsx')

        elif command == CommandEnum.SAVE_CLEAN_DATA:
            self._save_clean_data()
            return self._read_file(DataType.CleanedData, file_name)
        elif command == CommandEnum.ANALYZE_DATA:
            self._analyze_data()
            return self._read_file(DataType.AnalyzedData, file_name)
        else:
            print("Unknown command")



    def _read_file(self, data_type:DataType, file_name:str) -> DataFrame:
        base_dir = str(path.dirname(__file__))
        file_path = path.join(base_dir, '..\\', 'cached_data', data_type.value[0], file_name)
        df = pd.read_excel(file_path)
        return df

    def _read_power_plant_file(self):
        p_plant = PowerPLantService(84071569)
        p_plant.exec()
        df = self._read_file(DataType.PowerPlant, 'power-plant.xlsx')
        return df


    def _api_request(self, request_params: RequestParams):
        print('aici',request_params.latitude)

        lat = request_params.latitude
        lon = request_params.longitude
        start_date = request_params.start_date
        end_date = request_params.end_date

        if len(sys.argv) >= 3:
            try:
                lat = float(sys.argv[1])
                lon = float(sys.argv[2])
            except ValueError:
                print("Invalid latitude/longitude arguments. Using defaults.")

        if len(sys.argv) >= 4:
            start_date = sys.argv[3]
        if len(sys.argv) >= 5:
            end_date = sys.argv[4]

        requester = ApiRequest(latitude=lat, longitude=lon)

        url = "https://archive-api.open-meteo.com/v1/archive?"
        hourly_keys = [
            "is_day", "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
            "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code",
            "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
            "visibility", "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit",
            "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_direction_80m", "wind_direction_120m",
            "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "soil_temperature_0cm", "soil_temperature_6cm",
            "soil_temperature_18cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm",
            "shortwave_radiation", "direct_radiation", "diffuse_radiation", "direct_normal_irradiance",
            "global_tilted_irradiance", "terrestrial_radiation", "shortwave_radiation_instant",
            "direct_radiation_instant", "diffuse_radiation_instant", "direct_normal_irradiance_instant",
            "global_tilted_irradiance_instant", "terrestrial_radiation_instant"
        ]

        try:
            extra_params: dict[str, str] = {"wind_speed_unit": "ms"}

            if start_date:
                extra_params["start_date"] = start_date
            if end_date:
                extra_params["end_date"] = end_date

            responses = requester.fetch_openmeteo(
                url=url,
                hourly=hourly_keys,
                extra_params=extra_params if extra_params else {},
            )
            self._save_darty_data(responses[0], hourly_keys)

        except Exception as e:
            print(f"Open-Meteo request failed: {e}")
            return



    def _save_darty_data(self, response, hourly_keys):
        parser = OpenMeteoParser(response)
        df = parser.to_dataframe(hourly_keys)
        saved_data = SaveData(file_name='weather_data', data_type=DataType.DirtyData)
        saved_data.save(df)
        info_data = Analysis(df)
        summary_stats = info_data.summary_statistics()
        save_stats = SaveData(file_name='summary_statistics', data_type=DataType.DirtyData)
        save_stats.save(summary_stats)
        corr_matrix = info_data.correlation_matrix()
        save_matrix = SaveData(file_name='corr_matrix', data_type=DataType.DirtyData)
        save_matrix.save(corr_matrix)

    def _join_data(self):
        df_dirty = self._read_file(DataType.DirtyData, file_name='weather_data.xlsx')
        df_power = self._read_file(DataType.PowerPlant, file_name='power-plant.xlsx')
        df_dirty['datetime'] = pd.to_datetime(
            df_dirty['date'].astype(str) + ' ' + df_dirty['hour'].astype(str) + ':00'
        )
        df_power['datetime'] = pd.to_datetime(df_power['Date'])

        merged_df = pd.merge(
            df_power,
            df_dirty,
            on='datetime',
            how='inner'
        )
        save_obj = SaveData(file_name='weather-power', data_type=DataType.JoinData)
        save_obj.save(merged_df)

    def _save_clean_data(self, ):
        df = self._read_file(DataType.JoinData, file_name='weather-power.xlsx')
        cleaner = DataCleaner(raw_data=df)
        clean_data = cleaner.clean()
        saved_cleaned_data = SaveData(file_name='weather_data', data_type=DataType.CleanedData)
        saved_cleaned_data.save(pd.DataFrame(clean_data))
        info_data = Analysis(clean_data)
        summary_stats = info_data.summary_statistics()
        save_stats = SaveData(file_name='summary_statistics', data_type=DataType.CleanedData)
        save_stats.save(summary_stats)
        corr_matrix = info_data.correlation_matrix()
        save_matrix = SaveData(file_name='corr_matrix', data_type=DataType.CleanedData)
        save_matrix.save(corr_matrix)


    def _analyze_data(self):
        df = self._read_file(DataType.CleanedData, file_name='weather_data.xlsx')
        feature_engineer = FeatureEngineer(df)
        data_fe = feature_engineer.execute()
        saved_data_fe = SaveData(file_name='weather_data', data_type=DataType.AnalyzedData)
        saved_data_fe.save(data_fe)
        info_data = Analysis(data_fe)
        summary_stats = info_data.summary_statistics()
        save_stats = SaveData(file_name='summary_statistics', data_type=DataType.AnalyzedData)
        save_stats.save(summary_stats)
        corr_matrix = info_data.correlation_matrix()
        save_matrix = SaveData(file_name='corr_matrix', data_type=DataType.AnalyzedData)
        save_matrix.save(corr_matrix)


