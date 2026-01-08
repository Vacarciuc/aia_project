from __future__ import annotations
from typing import Any, Optional
import json
import logging
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests


class ApiRequest:
    def __init__(self, latitude: float, longitude: float) -> None:
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
        try:
            self.session.close()
        except Exception:
            pass

