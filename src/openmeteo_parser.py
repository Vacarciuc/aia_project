from __future__ import annotations

from typing import Sequence, List, Dict, Any

try:
    import pandas as pd
except Exception:
    pd = None


class OpenMeteoParser:
    """
    Clasa care transformă răspunsul brut Open-Meteo în structuri tabulare (liste de rânduri sau DataFrame).

    Atribute:
    - response: Obiectul de răspuns de la clientul Open-Meteo, cu metode precum Hourly(), Latitude(), Longitude().

    Flux tipic:
    1) Instanțiere cu răspunsul brut.
    2) Apelul `to_rows` pentru o listă de dict-uri (independent de pandas).
    3) Apelul `to_dataframe` pentru un DataFrame pandas (dacă pandas este instalat).
    """
    def __init__(self, response) -> None:
        """
        Initializează parser-ul cu un obiect de răspuns Open-Meteo.

        Parametri:
        - response: Obiectul de răspuns returnat de clientul openmeteo_requests.

        Ce se întâmplă în interior:
        - Salvează referința la răspuns pentru utilizare ulterioară.

        Ieșire:
        - None
        """
        self.response = response

    def _time_index(self):
        """
        Construiește tripleta (start, end, interval) a axei temporale pentru datele orare.

        Parametri:
        - None (folosește self.response)

        Ce se întâmplă în interior:
        - Extrage din `Hourly()` valorile Time (epoch sec), TimeEnd (epoch sec) și Interval (secunde).

        Ieșire:
        - Tuple (start: int, end: int, interval: int)
        """
        hourly = self.response.Hourly()
        start = hourly.Time()
        end = hourly.TimeEnd()
        interval = hourly.Interval()
        return start, end, interval

    def to_rows(self, hourly_keys: Sequence[str]) -> List[Dict[str, Any]]:
        """
        Convertește datele orare într-o listă de rânduri (dict-uri) cu câmpuri cheie.

        Parametri:
        - hourly_keys (Sequence[str]): Numele variabilelor orare de extras (în ordinea variabilelor din răspuns).

        Ce se întâmplă în interior:
        - Obține obiectul `Hourly()` și indexul temporal (start, end, interval).
        - Construiește lista de timestamp-uri (epoch sec) cu `range`.
        - Preia valorile variabilelor ca numpy arrays folosind `Variables(i).ValuesAsNumpy()`.
        - Calculează minimul m al lungimilor pentru a evita out-of-range.
        - Construiește rândurile cu chei standard: date (epoch sec), hour (None), latitude, longitude, plus variabilele cerute.

        Ieșire:
        - List[Dict[str, Any]]: Fiecare dict reprezintă un moment orar cu valorile variabilelor.
        """
        r = self.response
        hourly = r.Hourly()
        start, end, interval = self._time_index()
        # Build timestamps in seconds
        times = list(range(start, end, interval))
        rows: List[Dict[str, Any]] = []
        # Pre-fetch values arrays
        values_series = [hourly.Variables(i).ValuesAsNumpy() for i in range(len(hourly_keys))]
        m = min(len(times), *[len(v) for v in values_series]) if values_series else len(times)
        for idx in range(m):
            ts = times[idx]
            row: Dict[str, Any] = {
                "date": ts,
                "hour": None,
                "latitude": r.Latitude(),
                "longitude": r.Longitude(),
            }
            for i, key in enumerate(hourly_keys):
                row[key] = values_series[i][idx]
            rows.append(row)
        return rows

    def to_dataframe(self, hourly_keys: Sequence[str]):
        """
        Convertește datele orare într-un `pandas.DataFrame` cu coloane gata de analiză.

        Parametri:
        - hourly_keys (Sequence[str]): Numele variabilelor orare de extras.

        Ce se întâmplă în interior:
        - Validează că `pandas` este disponibil; altfel ridică eroare cu instrucțiunea de fallback.
        - Construiește indexul temporal folosind epoch sec -> UTC (date_range cu freq în secunde).
        - Creează DataFrame inițial cu coloana `time`.
        - Pentru fiecare cheie orară, inserează vectorul de valori; dacă dimensiunile nu coincid,
          ajustează tăind la minimul comun.
        - Adaugă lat/long constante pe toate rândurile.
        - Derivă coloanele `date` (YYYY-MM-DD) și `hour` (HH:MM) din `time`.
        - Reordonare coloane astfel încât să fie: [date, hour, latitude, longitude, <metrice>].

        Ieșire:
        - pandas.DataFrame: Tabelul final cu variabilele cerute și informația temporală/geografică.
        """
        if pd is None:
            raise RuntimeError("pandas is not installed; use to_rows() instead.")
        r = self.response
        hourly = r.Hourly()
        # Build the time index
        date_index = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
        df = pd.DataFrame({"time": date_index})
        # Add variables
        for i, key in enumerate(hourly_keys):
            values = hourly.Variables(i).ValuesAsNumpy()
            if len(values) != len(df):
                m = min(len(values), len(df))
                df = df.iloc[:m].reset_index(drop=True)
                values = values[:m]
            df[key] = values
        df["latitude"] = r.Latitude()
        df["longitude"] = r.Longitude()
        df["date"] = df["time"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["time"].dt.strftime("%H:%M")
        leading = ["date", "hour", "latitude", "longitude"]
        metrics = [c for c in df.columns if c not in set(leading + ["time"])]
        return df[leading + metrics]
