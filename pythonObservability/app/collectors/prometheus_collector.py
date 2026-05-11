import requests

PROMETHEUS_URL = "http://prometheus:9090"


class PrometheusCollector:

    @staticmethod
    def query(query: str) -> dict:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def query_range(query: str, start: int, end: int, step: int = 60) -> dict:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={"query": query, "start": start, "end": end, "step": step},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
