import requests

PROMETHEUS_URL = "http://prometheus:9090"


class PrometheusCollector:

    @staticmethod
    def query(query: str):

        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query}
        )

        response.raise_for_status()

        return response.json()