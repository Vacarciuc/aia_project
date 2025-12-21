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
          apelează `_clean_dataframe`.
        - Dacă `raw_data` este listă de dict-uri (sau listă goală), apelează `_clean_rows`.
        - În orice alt caz, returnează `raw_data` neschimbat.

        Ieșire:
        - Obiectul curățat: `pd.DataFrame`, `list[dict]` sau orice alt tip inițial.
        """
        if pd is not None and isinstance(self.raw_data, pd.DataFrame):
            return self._clean_dataframe(self.raw_data)
        if isinstance(self.raw_data, list) and (len(self.raw_data) == 0 or isinstance(self.raw_data[0], dict)):
            return self._clean_rows(self.raw_data)
        return self.raw_data

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
        if "latitude" in cleaned:
            cleaned["latitude"] = pd.to_numeric(cleaned["latitude"], errors="coerce").fillna(0)
        if "longitude" in cleaned:
            cleaned["longitude"] = pd.to_numeric(cleaned["longitude"], errors="coerce").fillna(0)
        return cleaned.reset_index(drop=True)

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
