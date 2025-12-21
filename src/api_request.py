from __future__ import annotations
from typing import Any, Optional
import json
import logging
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests


class ApiRequest:
    """
    Clasa responsabilă pentru efectuarea cererilor HTTP și interacțiunea cu clientul Open-Meteo.

    Atribute:
    - latitude (float): Latitudinea locației țintă.
    - longitude (float): Longitudinea locației țintă.
    - session (requests.Session): Sesiune HTTP reutilizabilă pentru cereri generice.
    - cache_session (requests_cache.CachedSession): Sesiune cu cache pentru a reduce traficul.
    - retry_session: Sesiune cu mecanism de retry (reîncercare) pentru robustețe.
    - openmeteo (openmeteo_requests.Client): Client Open-Meteo ce folosește sesiunea cu retry.

    Utilizare tipică:
    1) Instanțiere cu lat/lon.
    2) Apelul `fetch` pentru endpoint-uri arbitrare (JSON).
    3) Apelul `fetch_openmeteo` pentru API-ul Open-Meteo cu parametri orari.
    4) Apelul `close` pentru a închide sesiunea.
    """

    def __init__(self, latitude: float, longitude: float) -> None:
        """
        Initializează clasa cu coordonatele geografice și pregătește sesiunile HTTP.

        Parametri:
        - latitude: Latitudinea locației, în grade zecimale.
        - longitude: Longitudinea locației, în grade zecimale.

        Ce se întâmplă în interior:
        - Creează o sesiune `requests.Session`.
        - Configurează o sesiune cu cache (expiră după 3600s).
        - Configurează mecanismul de retry pentru cereri.
        - Creează clientul Open-Meteo care folosește sesiunea cu retry.

        Ieșire:
        - None (setează atributele interne).
        """
        self.latitude = latitude
        self.longitude = longitude
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=self.retry_session)

    def fetch(
        self,
        *,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        timeout: int = 10,
        headers: Optional[dict[str, str]] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Efectuează o cerere GET către un endpoint arbitrar și returnează răspuns JSON.

        Parametri (toți sunt keyword-only):
        - endpoint (str): URL-ul complet către care se face cererea.
        - params (dict, opțional): Parametri de query adiționali. Latitudinea și
          longitudinea sunt adăugate implicit dacă nu există.
        - timeout (int): Timpul maxim de așteptare (secunde). Implicit 10s.
        - headers (dict, opțional): Antete HTTP suplimentare.

        Ce se întâmplă în interior:
        - Construiește dicționarul final de parametri, adăugând `latitude` și
          `longitude` dacă lipsesc.
        - Trimite cererea cu `requests.Session.get` și verifică statusul.
        - Încearcă să parseze răspunsul ca JSON; dacă eșuează, îl parsează manual
          din text.
        - Loghează erorile și propagă excepțiile relevante.

        Ieșire:
        - dict (JSON) cu răspunsul, dacă parsearea reușește; altfel poate ridica
          excepții. Tipul returnat este Optional[dict] doar pentru compatibilitate
          cu fluxul existent; în practică, dacă cererea reușește, se întoarce un dict.
        """

        try:
            final_params = params.copy() if params else {}
            final_params.setdefault("latitude", self.latitude)
            final_params.setdefault("longitude", self.longitude)
            resp = self.session.get(endpoint, params=final_params, timeout=timeout, headers=headers)
            resp.raise_for_status()

            try:
                return resp.json()
            except ValueError:
                return json.loads(resp.text)
        except requests.RequestException as re:
            self.logger.error("Request error: %s", re)
            raise
        except Exception as e:
            self.logger.error("Unexpected error: %s", e)
            raise

    def fetch_openmeteo(
        self,
        *,
        url: str,
        hourly: list[str],
        current: Optional[list[str]] = None,
        extra_params: Optional[dict[str, Any]] = None,
    ):
        """
        Interoghează API-ul Open-Meteo folosind clientul dedicat și returnează răspunsurile brute.

        Parametri (keyword-only):
        - url (str): Endpoint-ul Open-Meteo (ex: "https://archive-api.open-meteo.com/v1/archive?").
        - hourly (list[str]): Chei/variabile orare solicitate (ex: temperature_2m, rain).
        - current (list[str], opțional): Variabile curente solicitate.
        - extra_params (dict, opțional): Parametri suplimentari (ex: start_date, end_date, timezone etc.).

        Ce se întâmplă în interior:
        - Construiește dicționarul de parametri cu lat/lon și cheile `hourly`.
        - Adaugă `current` dacă a fost furnizat și îmbină `extra_params` dacă există.
        - Apelează `self.openmeteo.weather_api(url, params=params)` pentru a obține
          lista de răspunsuri.

        Ieșire:
        - O colecție de răspunsuri provenite de la clientul Open-Meteo (de regulă o
          listă de obiecte de răspuns specific bibliotecii openmeteo_requests).
        """

        params: dict[str, Any] = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": hourly,
        }
        if current:
            params["current"] = current
        if extra_params:
            params.update(extra_params)

        responses = self.openmeteo.weather_api(url, params=params)
        return responses

    def close(self) -> None:
        """
        Închide sesiunea HTTP internă pentru a elibera resursele.

        Parametri:
        - None

        Ce se întâmplă în interior:
        - Apelează `self.session.close()` într-un bloc try/except pentru a evita
          propagarea excepțiilor necritice.

        Ieșire:
        - None
        """
        try:
            self.session.close()
        except Exception:
            pass

