from __future__ import annotations

from typing import Sequence, List, Dict, Any

try:
    import pandas as pd
except Exception:
    pd = None


class OpenMeteoParser:
    def __init__(self, response) -> None:
        self.response = response

    def _time_index(self):
        hourly = self.response.Hourly()
        start = hourly.Time()
        end = hourly.TimeEnd()
        interval = hourly.Interval()
        return start, end, interval

    def to_rows(self, hourly_keys: Sequence[str]) -> List[Dict[str, Any]]:
        r = self.response
        hourly = r.Hourly()
        start, end, interval = self._time_index()
        times = list(range(start, end, interval))
        rows: List[Dict[str, Any]] = []
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
        if pd is None:
            raise RuntimeError("pandas is not installed; use to_rows() instead.")
        r = self.response
        hourly = r.Hourly()
        date_index = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
        df = pd.DataFrame({"time": date_index})
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
