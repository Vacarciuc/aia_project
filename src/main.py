import sys
import pandas as pd

from src.api_request import ApiRequest
from src.openmeteo_parser import OpenMeteoParser
from src.data_cleaner import DataCleaner
from src.save_data import SaveData, DataType



def main() -> None:
    lat: float = 47.0269
    lon: float = 28.8416
    start_date: str = "2020-01-01"
    end_date: str = "2026-01-01"


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
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "rain",
        "precipitation_probability",
        "visibility",
        "wind_speed_10m",
        "cloud_cover",
        "surface_pressure",
    ]
    try:
        extra_params: dict[str, str] = {}
        if start_date:
            extra_params["start_date"] = start_date
        if end_date:
            extra_params["end_date"] = end_date

        responses = requester.fetch_openmeteo(
            url=url,
            hourly=hourly_keys,
            current=["cloud_cover"],
            extra_params=extra_params if extra_params else {},
        )
    except Exception as e:
        print(f"Open-Meteo request failed: {e}")
        return

    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone offset: {response.UtcOffsetSeconds()} s")

    # Parse into tabular format
    parser = OpenMeteoParser(response)
    try:
        df = parser.to_dataframe(hourly_keys)
        saved_data = SaveData(file_name='test1', data_type=DataType.DartyData)
        saved_data.save(df)
        cleaner = DataCleaner(raw_data=df)
        clean_data = cleaner.clean()
        saved_cleaned_data = SaveData(file_name='test1_cleaned', data_type=DataType.CleanedData)
        saved_cleaned_data.save(pd.DataFrame(clean_data))
    except Exception as e:
        print(e)
        rows = parser.to_rows(hourly_keys)
        cleaner = DataCleaner(raw_data=rows)
        cleaned = cleaner.clean()






if __name__ == '__main__':
    main()