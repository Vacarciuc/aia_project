from __future__ import annotations

from typing import Any, Optional, Sequence

try:
    import pandas as pd
    import numpy as np
except Exception:
    pd = None
    np = None


class DataCleaner:
    def __init__(self, raw_data: Any) -> None:
        self.raw_data = raw_data

    def clean(self) -> Optional[Any]:
        if pd is not None and isinstance(self.raw_data, pd.DataFrame):
            df = self._ensure_types_df(self.raw_data)
            df = self._handle_missing_df(df)
            df = self._drop_empty_columns_df(df)
            df = self._remove_duplicates_df(df)
            df = self._handle_outliers_df(df)
            return df.reset_index(drop=True)
        if isinstance(self.raw_data, list) and (len(self.raw_data) == 0 or isinstance(self.raw_data[0], dict)):
            rows = self._ensure_types_rows(self.raw_data)
            rows = self._handle_missing_rows(rows)
            rows = self._drop_empty_columns_rows(rows)
            rows = self._remove_duplicates_rows(rows)
            rows = self._handle_outliers_rows(rows)
            return rows
        return self.raw_data

    def notna(self, v: Any) -> bool:
        return not self._is_nan_like(v)

    def dropna(self, seq: Sequence[Any]) -> list[Any]:
        return [x for x in seq if self.notna(x)]

    def _measurement_columns(self, columns: Sequence[str]) -> list[str]:
        leading = {"date", "hour", "latitude", "longitude", "time"}
        return [c for c in columns if c not in leading]

    def _ensure_types_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        if pd is None:
            return df
        cols = self._measurement_columns(df.columns.tolist())
        if not cols:
            return df.copy()
        out = df.copy()
        for c in cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        for k in ("latitude", "longitude"):
            if k in out.columns:
                out[k] = pd.to_numeric(out[k], errors="coerce")
        return out

    def _handle_missing_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        if pd is None:
            return df
        cols = self._measurement_columns(df.columns.tolist())
        if not cols:
            return df.copy()
        out = df.copy()
        out = out.dropna(subset=cols, how="all")
        out[cols] = out[cols].fillna(0)
        for k in ("latitude", "longitude"):
            if k in out.columns:
                out[k] = out[k].fillna(0)
        return out

    def _drop_empty_columns_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        if pd is None:
            return df
        keep_meta = {"date", "hour", "latitude", "longitude", "time"}
        out = df.copy()
        drop = []
        for c in out.columns:
            if c in keep_meta:
                continue
            series = out[c]
            # true dacă toate valorile sunt NaN sau 0
            all_empty = bool(((series.isna()) | (series == 0)).all())
            if all_empty:
                drop.append(c)
        if drop:
            out = out.drop(columns=drop)
        return out

    def _remove_duplicates_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        if pd is None:
            return df
        return df.drop_duplicates()

    def _handle_outliers_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        if pd is None:
            return df
        cols = self._measurement_columns(df.columns.tolist())
        if not cols:
            return df
        out = df.copy()
        numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(out[c])]
        if not numeric_cols:
            return out
        q_low = out[numeric_cols].quantile(0.01)
        q_high = out[numeric_cols].quantile(0.99)
        for c in numeric_cols:
            low = q_low[c]
            high = q_high[c]
            # Convert to float if possible for robust comparisons
            try:
                low_f = float(low)
            except Exception:
                low_f = None
            try:
                high_f = float(high)
            except Exception:
                high_f = None
            if low_f is not None and high_f is not None and low_f < high_f:
                out[c] = out[c].clip(lower=low_f, upper=high_f)
        return out

    def _is_nan_like(self, v: Any) -> bool:
        if v is None:
            return True
        if np is not None:
            try:
                return bool(np.isnan(v))
            except Exception:
                return False
        try:
            return v != v
        except Exception:
            return False

    def _measurement_keys_rows(self, rows: list[dict[str, Any]]) -> list[str]:
        if not rows:
            return []
        return self._measurement_columns(list(rows[0].keys()))

    def _ensure_types_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        measure_keys = self._measurement_keys_rows(rows)
        out: list[dict[str, Any]] = []
        for r in rows:
            new_r = dict(r)
            for k in measure_keys:
                v = new_r.get(k)
                try:
                    new_r[k] = float(v) if v is not None else float('nan')
                except Exception:
                    new_r[k] = float('nan')
            for k in ("latitude", "longitude"):
                if k in new_r:
                    try:
                        new_r[k] = float(new_r[k])
                    except Exception:
                        new_r[k] = 0.0
            out.append(new_r)
        return out

    def _handle_missing_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        measure_keys = self._measurement_keys_rows(rows)
        def all_measures_nan(r: dict[str, Any]) -> bool:
            return all(self._is_nan_like(r.get(k)) for k in measure_keys)
        out: list[dict[str, Any]] = []
        for r in rows:
            if all_measures_nan(r):
                continue
            new_r = dict(r)
            for k in measure_keys:
                v = new_r.get(k)
                try:
                    fv = float(v) if v is not None else float('nan')
                except Exception:
                    fv = float('nan')
                if fv != fv:  # NaN check
                    fv = 0.0
                new_r[k] = fv
            for k in ("latitude", "longitude"):
                if k in new_r:
                    try:
                        val = float(new_r[k])
                        if val != val:
                            val = 0.0
                    except Exception:
                        val = 0.0
                    new_r[k] = val
            out.append(new_r)
        return out

    def _drop_empty_columns_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        keep_meta = {"date", "hour", "latitude", "longitude", "time"}
        keys = list(rows[0].keys())
        to_check = [k for k in keys if k not in keep_meta]
        drop_keys: list[str] = []
        for k in to_check:
            all_empty = True
            for r in rows:
                v = r.get(k)
                is_empty = (self._is_nan_like(v) or v == 0)
                if not is_empty:
                    all_empty = False
                    break
            if all_empty:
                drop_keys.append(k)
        if not drop_keys:
            return rows
        out: list[dict[str, Any]] = []
        for r in rows:
            new_r = {kk: vv for kk, vv in r.items() if kk not in drop_keys}
            out.append(new_r)
        return out

    def _remove_duplicates_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen = set()
        out: list[dict[str, Any]] = []
        for r in rows:
            key = tuple(sorted(r.items()))
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out

    def _handle_outliers_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        measure_keys = self._measurement_keys_rows(rows)
        if not measure_keys:
            return rows
        def percentiles(vals: list[float]) -> tuple[float, float]:
            if pd is not None:
                s = pd.Series(vals)
                return float(s.quantile(0.01)), float(s.quantile(0.99))
            vs = sorted(v for v in vals if v == v)
            if not vs:
                return (float('-inf'), float('inf'))
            n = len(vs)
            i1 = max(0, int(0.01 * (n - 1)))
            i9 = max(0, int(0.99 * (n - 1)))
            return vs[i1], vs[i9]
        per_bounds: dict[str, tuple[float, float]] = {}
        for k in measure_keys:
            vals = [float(r.get(k, 0.0)) for r in rows]
            low, high = percentiles(vals)
            per_bounds[k] = (low, high)
        out: list[dict[str, Any]] = []
        for r in rows:
            new_r = dict(r)
            for k in measure_keys:
                v = new_r.get(k)
                try:
                    fv = float(v)
                except Exception:
                    fv = 0.0
                low, high = per_bounds[k]
                if low < high:
                    if fv < low:
                        fv = low
                    elif fv > high:
                        fv = high
                new_r[k] = fv
            out.append(new_r)
        return out

    def _clean_dataframe(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        cols = self._measurement_columns(df.columns.tolist())
        if not cols:
            return df.copy()
        cleaned = df.copy()
        cleaned = cleaned.dropna(subset=cols, how="all")
        for c in cols:
            cleaned[c] = pd.to_numeric(cleaned[c], errors="coerce")
        cleaned[cols] = cleaned[cols].fillna(0)
        if pd is not None and "latitude" in cleaned.columns:
            cleaned["latitude"] = pd.to_numeric(cleaned["latitude"], errors="coerce")
            cleaned["latitude"] = cleaned["latitude"].fillna(0)
        if pd is not None and "longitude" in cleaned.columns:
            cleaned["longitude"] = pd.to_numeric(cleaned["longitude"], errors="coerce")
            cleaned["longitude"] = cleaned["longitude"].fillna(0)
        return cleaned.reset_index(drop=True)

    def _clean_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        keys = list(rows[0].keys())
        measure_keys = self._measurement_columns(keys)
        def all_measures_nan(r: dict[str, Any]) -> bool:
            return all(self._is_nan_like(r.get(k)) for k in measure_keys)
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            if all_measures_nan(r):
                continue
            new_r = dict(r)
            for k in measure_keys:
                v = new_r.get(k)
                try:
                    fv = float(v) if v is not None else float('nan')
                except Exception:
                    fv = float('nan')
                if fv != fv:
                    fv = 0.0
                new_r[k] = fv
            for k in ("latitude", "longitude"):
                if k in new_r:
                    try:
                        new_r[k] = float(new_r[k])
                    except Exception:
                        new_r[k] = 0.0
            cleaned.append(new_r)
        return cleaned
