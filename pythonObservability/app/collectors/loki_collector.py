import requests
from fastapi import HTTPException

LOKI_URL = "http://loki:3100"


class LokiCollector:

    @staticmethod
    def _get(params: dict, endpoint: str = "/loki/api/v1/query_range") -> dict:
        url = f"{LOKI_URL}{endpoint}"
        try:
            response = requests.get(url, params=params, timeout=30)
            if not response.ok:
                raise HTTPException(
                    status_code=502,
                    detail={"loki_status": response.status_code, "loki_error": response.text},
                )
            return response.json()
        except requests.exceptions.ConnectionError:
            raise HTTPException(status_code=503, detail="Loki unreachable")
        except requests.exceptions.Timeout:
            raise HTTPException(status_code=504, detail="Loki timeout")

    @staticmethod
    def query(query: str) -> dict:
        return LokiCollector._get({"query": query, "limit": 1000})

    @staticmethod
    def query_metric_range(query: str, start_s: int, end_s: int, step: int = 60) -> dict:
        return LokiCollector._get({
            "query": query,
            "start": start_s,
            "end": end_s,
            "step": step,
        })

    @staticmethod
    def get_raw_log_lines(start_s: int, end_s: int, limit: int = 10) -> list[str]:
        """
        Retorna as últimas `limit` linhas brutas de log nginx na janela [start_s, end_s].
        Usa o endpoint /query_range com direção reversa para obter as mais recentes.
        Retorna lista vazia em caso de erro para não interromper o pipeline.
        """
        try:
            raw = LokiCollector._get({
                "query": '{job="nginx", type="access"}',
                "start": start_s,
                "end": end_s,
                "limit": limit,
                "direction": "backward",
            })
            lines = []
            for stream in raw.get("data", {}).get("result", []):
                for _ts, line in stream.get("values", []):
                    lines.append(line)
                    if len(lines) >= limit:
                        return lines
            return lines
        except Exception:
            return []
