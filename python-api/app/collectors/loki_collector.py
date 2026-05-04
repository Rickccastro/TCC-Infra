# loki_collector.py
import requests
from fastapi import HTTPException

LOKI_URL = "http://loki:3100"

class LokiCollector:

    @staticmethod
    def query(query: str):
        url = f"{LOKI_URL}/loki/api/v1/query_range"
        params = {"query": query, "limit": 1000}

        try:
            response = requests.get(url, params=params, timeout=30)

            if not response.ok:
                # Isso vai te mostrar o erro REAL do Loki
                raise HTTPException(
                    status_code=502,
                    detail={
                        "loki_status": response.status_code,
                        "loki_error": response.text,  # <-- aqui está o motivo real
                    },
                )

            return response.json()

        except requests.exceptions.ConnectionError:
            raise HTTPException(status_code=503, detail="Loki unreachable")
        except requests.exceptions.Timeout:
            raise HTTPException(status_code=504, detail="Loki timeout")
