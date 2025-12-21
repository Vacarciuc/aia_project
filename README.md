# AIA Project

A small, modular Python project that fetches historical weather data from Open-Meteo, parses it into a tabular format, and applies basic cleaning. It’s designed as a clean baseline to experiment with data pipelines using latitude/longitude context and a simple CLI.

## Key Features
- HTTP requests with latitude/longitude context via `ApiRequest`
- Parsing of Open-Meteo hourly responses into rows or a pandas DataFrame via `OpenMeteoParser`
- Basic data cleaning for DataFrame or list-of-rows via `DataCleaner`
- Simple CLI in `src/main.py` to demonstrate end-to-end flow

## Architecture Overview
- `src/api_request.py` — Wraps HTTP calls and integrates the Open-Meteo client with caching and retries.
- `src/openmeteo_parser.py` — Converts raw Open-Meteo responses to tabular data (rows or pandas DataFrame).
- `src/data_cleaner.py` — Applies simple normalization and NaN handling to the parsed data.
- `src/main.py` — CLI entry point: requests data, parses, cleans, and prints previews.

## Requirements
- Python 3.11+ recommended
- Dependencies listed in `requirements.txt` (requests, requests-cache, retry-requests, openmeteo-requests, pandas optional)

## Setup (Windows PowerShell)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Usage
Run with default coordinates or provide your own:
```powershell
# Default demo (NYC coordinates in code if no args are provided)
python src\main.py

# Specify latitude and longitude
python src\main.py 40.7128 -74.0060

# Specify latitude, longitude, start_date, end_date (YYYY-MM-DD)
python src\main.py 47.0269 28.8416 2015-12-01 2025-12-01
```

On first run, the project queries the Open-Meteo Archive API, attempts to parse into a pandas DataFrame (if installed), falls back to a list of rows otherwise, and applies basic cleaning before printing a preview.

## Typical Flow
1. `ApiRequest.fetch_openmeteo` retrieves hourly variables for a given lat/lon and date range.
2. `OpenMeteoParser.to_dataframe` or `.to_rows` converts the response to tabular data.
3. `DataCleaner.clean` normalizes values and handles NaNs.
4. `main.py` prints previews of parsed and cleaned data.

## Next Steps
- Decide target API and authentication
- Expand cleaning logic and add domain-specific validations
- Add tests and structured logging
- Optionally, generate API documentation from docstrings (e.g., with Sphinx or pdoc)
