from __future__ import annotations

from typing import Any, Optional, Sequence

try:
    import pandas as pd
    import numpy as np
except Exception:
    pd = None
    np = None


class DataCleaner:
    """
    Clasa care aplică reguli simple de curățare asupra datelor meteo, atât pentru DataFrame, cât și pentru liste de rânduri.

    Atribute:
    - raw_data (Any): Datele brute de intrare, fie un pandas.DataFrame, fie listă de dict-uri (rânduri), fie alt tip.

    Comportament:
    - Dacă `raw_data` este DataFrame, normalizează coloanele numerice, elimină rândurile complet NaN pe măsuri și umple NaN cu 0.
    - Dacă `raw_data` este listă de dict-uri, filtrează rândurile complet NaN pe măsuri, convertește valorile la float și normalizează lat/lon.
    - Altfel, returnează datele așa cum sunt.
    """

    def __init__(self, raw_data: Any) -> None:
        """
        Constructor simplu care reține datele brute.

        Parametri:
        - raw_data (Any): Datele brute de intrare.

        Ce se întâmplă în interior:
        - Salvează `raw_data` într-un atribut pentru utilizare ulterioară.

        Ieșire:
        - None
        """
        self.raw_data = raw_data

    def clean(self) -> Optional[Any]:
        """
        Rulează fluxul de curățare corespunzător tipului de date.

        Parametri:
        - None (folosește `self.raw_data`).

        Ce se întâmplă în interior:
        - Detectează dacă `raw_data` este DataFrame (și pandas este disponibil), caz în care
          aplică pașii: tipuri, lipsă, duplicate, outlieri.
        - Dacă `raw_data` este listă de dict-uri (sau listă goală), aplică pașii echivalenți pe rânduri.
        - În orice alt caz, returnează `raw_data` neschimbat.

        Ieșire:
        - Obiectul curățat: `pd.DataFrame`, `list[dict]` sau orice alt tip inițial.
        """
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

    # --- Helperuri generale ---
    def notna(self, v: Any) -> bool:
        """
        Testează dacă o valoare NU este NaN-like.
        - Returnează False pentru None sau NaN.
        - True pentru orice altă valoare.
        """
        return not self._is_nan_like(v)

    def dropna(self, seq: Sequence[Any]) -> list[Any]:
        """
        Elimină valorile NaN-like dintr-o secvență.
        """
        return [x for x in seq if self.notna(x)]

    def _measurement_columns(self, columns: Sequence[str]) -> list[str]:
        """
        Identifică coloanele de măsurători, excluzând metadatele standard.

        Parametri:
        - columns (Sequence[str]): Lista tuturor numelor de coloane.

        Ce se întâmplă în interior:
        - Elimină din listă câmpurile meta: date, hour, latitude, longitude, time.

        Ieșire:
        - list[str]: Numai coloanele de măsurători.
        """
        leading = {"date", "hour", "latitude", "longitude", "time"}
        return [c for c in columns if c not in leading]

    # --- Pași pe DataFrame ---
    def _ensure_types_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        """
        Asigură tipuri corecte pe DataFrame:
        - Coloanele de măsurători sunt convertite numeric (invalid -> NaN).
        - Latitude/longitude convertite numeric.
        """
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
        """
        Gestionează valori lipsă pe DataFrame:
        - Elimină rândurile unde TOATE măsurile sunt NaN.
        - Umple NaN din măsurători cu 0.
        - Umple NaN din latitude/longitude cu 0.
        """
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
        """
        Elimină coloanele unde toate valorile sunt NaN sau 0.
        - Se aplică atât pe coloane de măsurători, cât și (opțional) pe altele, dar păstrează metadatele standard.
        """
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
        """
        Elimină duplicatele din DataFrame.
        - Definește duplicatele ca rânduri identice pe toate coloanele.
        """
        if pd is None:
            return df
        return df.drop_duplicates()

    def _handle_outliers_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        """
        Tratează outlierii într-o manieră simplă (conservatoare):
        - Clipping valorilor măsurate la un interval [p1, p99] pentru a limita extremitățile.
        - Evită modificări agresive; poate fi ajustat ulterior cu reguli de domeniu.
        """
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

    # --- Pași pe listă de rânduri ---
    def _is_nan_like(self, v: Any) -> bool:
        """
        Testează dacă o valoare este echivalentă cu NaN.

        Parametri:
        - v (Any): Valoarea de testat.

        Ce se întâmplă în interior:
        - Verifică None.
        - Dacă numpy este disponibil, încearcă `np.isnan` pe valoare.
        - Fallback: folosește proprietatea NaN != NaN pentru a detecta NaN.

        Ieșire:
        - bool: True dacă valoarea este None/NaN, False altfel.
        """
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
        """
        Identifică cheile de măsurători pentru listă de rânduri folosind primul rând.
        """
        if not rows:
            return []
        return self._measurement_columns(list(rows[0].keys()))

    def _ensure_types_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Asigură tipuri corecte pe rânduri:
        - Convertește valorile măsurate la float (invalid -> NaN).
        - Latitude/longitude la float.
        """
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
        """
        Gestionează valori lipsă pe rânduri:
        - Elimină rândurile unde TOATE măsurile sunt NaN-like.
        - Umple NaN din măsurători cu 0.
        - Umple NaN din latitude/longitude cu 0.
        """
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
        """
        Elimină cheile (coloanele) unde toate valorile sunt NaN-like sau 0 în lista de rânduri.
        - Păstrează metadatele standard: date, hour, latitude, longitude, time.
        """
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
        """
        Elimină duplicatele din lista de rânduri.
        - Folosește transformarea la tuplu ordonat de (cheie, valoare) pentru a detecta rânduri identice.
        """
        seen = set()
        out: list[dict[str, Any]] = []
        for r in rows:
            # Folosește tuple sorted pentru stabilitate indiferent de ordinea cheilor
            key = tuple(sorted(r.items()))
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out

    def _handle_outliers_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Tratează outlierii (conservator) pe lista de rânduri:
        - Aplică clipping pe fiecare cheie de măsurătoare la intervalul [p1, p99] calculat pe valori numerice.
        """
        if not rows:
            return rows
        measure_keys = self._measurement_keys_rows(rows)
        if not measure_keys:
            return rows
        # Construiește serii pentru fiecare cheie
        def percentiles(vals: list[float]) -> tuple[float, float]:
            if pd is not None:
                s = pd.Series(vals)
                return float(s.quantile(0.01)), float(s.quantile(0.99))
            # Fallback simplu fără pandas: sortează și ia percentile aprox.
            vs = sorted(v for v in vals if v == v)
            if not vs:
                return (float('-inf'), float('inf'))
            n = len(vs)
            i1 = max(0, int(0.01 * (n - 1)))
            i9 = max(0, int(0.99 * (n - 1)))
            return vs[i1], vs[i9]
        # Calculează percentile și clip
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

    # --- Variantele anterioare (păstrate pentru compatibilitate) ---
    def _clean_dataframe(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        """
        Aplică pașii de curățare pe un DataFrame.

        Parametri:
        - df (pd.DataFrame): Tabelul inițial.

        Ce se întâmplă în interior:
        - Identifică coloanele de măsurători.
        - Elimină rândurile unde toate măsurile sunt NaN.
        - Convertește măsurile la numeric (coerce -> NaN pentru valori invalide).
        - Umple NaN cu 0 pentru măsurători.
        - Normalizează `latitude` și `longitude` la numeric cu NaN -> 0.
        - Resetează indexul.

        Ieșire:
        - pd.DataFrame: Copia curățată a tabelului.
        """
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
        """
        Aplică curățarea pe o listă de rânduri (dict-uri) cu chei standard.

        Parametri:
        - rows (list[dict[str, Any]]): Lista de rânduri inițiale.

        Ce se întâmplă în interior:
        - Dacă lista e goală, o returnează.
        - Identifică cheile de măsurători.
        - Filtrează rândurile unde toate măsurile sunt NaN-like.
        - Convertește valorile măsurate la float, invalide -> NaN -> 0.
        - Normalizează `latitude` și `longitude` la float cu fallback 0.

        Ieșire:
        - list[dict[str, Any]]: Lista curățată.
        """
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
