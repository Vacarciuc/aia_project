import sys

from src.api_request import ApiRequest
from src.openmeteo_parser import OpenMeteoParser
from src.data_cleaner import DataCleaner


def main() -> None:
    """
    Punctul de intrare al aplicației care interoghează Open-Meteo, parsează și curăță datele.

    Intrări:
    - Argumente linie de comandă (opționale):
      * sys.argv[1]: latitude (float)
      * sys.argv[2]: longitude (float)
      * sys.argv[3]: start_date (YYYY-MM-DD)
      * sys.argv[4]: end_date (YYYY-MM-DD)

    Ce se întâmplă în interior:
    - Citește argumentele; dacă lat/lon nu sunt valide, folosește valorile implicite.
    - Construiește lista de variabile orare de cerut.
    - Apelează clientul `ApiRequest.fetch_openmeteo` pentru a obține răspunsurile.
    - Afișează metadatele (coordonate, altitudine, offset de timp).
    - Parsează răspunsul în DataFrame (dacă pandas este disponibil) sau listă de rânduri.
    - Curăță datele cu `DataCleaner`.

    Ieșire:
    - Prin print: previzualizarea datelor parsate și rezultatul curățării.
    - Return: None.
    """
    lat: float = 47.0269
    lon: float = 28.8416
    start_date: str = "2015-12-01"
    end_date: str = "2025-12-01"

    if len(sys.argv) >= 3:
        try:
            lat = float(sys.argv[1])
            lon = float(sys.argv[2])
        except ValueError:
            print("[WARN] Invalid latitude/longitude arguments. Using defaults.")

    # Optional date range as YYYY-MM-DD; if provided, they will be passed to the API
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

    print("=== REQUEST (Open-Meteo client) ===")
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
        print(f"[ERROR] Open-Meteo request failed: {e}")
        return

    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone offset: {response.UtcOffsetSeconds()} s")

    # Parse into tabular format
    parser = OpenMeteoParser(response)
    try:
        df = parser.to_dataframe(hourly_keys)
        print("=== PARSED (TABULAR DataFrame) PREVIEW ===")
        print(df)
        print(f"Rows: {len(df)}, Columns: {list(df.columns)}")
        cleaner = DataCleaner(raw_data=df)
    except Exception:
        print("[INFO] pandas not available. Falling back to list-of-rows output.")
        rows = parser.to_rows(hourly_keys)
        print("=== PARSED (ROWS) PREVIEW ===")
        print(rows[:3])
        cleaner = DataCleaner(raw_data=rows)

    cleaned = cleaner.clean()

    print("=== CLEANED DATA ===")
    print(cleaned)


if __name__ == '__main__':
    main()