"""
Etapa 3 — Enriquecimento contextual dos eventos.

Adiciona metadados que ajudam a conectar eventos entre si:
host, endpoint e categoria da métrica. Não sobrescreve campos já existentes.
"""

from typing import List

_METRIC_CATEGORY = {
    "cpu_avg": "infrastructure",
    "mem_avg": "infrastructure",
    "rps": "traffic",
    "error_count": "errors",
    "p50_latency": "latency",
    "p95_latency": "latency",
    "p99_latency": "latency",
    "avg_response_time": "latency",
    "avg_upstream_time": "latency",
    "max_endpoint_p95": "latency",
    "login_attempts": "security",
    "login_avg_response": "security",
    "client_aborts": "errors",
    "http_2xx": "errors",
    "http_3xx": "errors",
    "http_4xx": "errors",
    "http_5xx": "errors",
    "mobile_count": "traffic",
    "desktop_count": "traffic",
}

# Mapeia cada métrica para o endpoint mais representativo da sua origem.
# Métricas de sistema (CPU, memória) usam "system".
# Métricas globais de tráfego usam "/*" para indicar cobertura ampla.
_METRIC_ENDPOINT = {
    "login_attempts":     "/login/index.php",
    "login_avg_response": "/login/index.php",
    "client_aborts":      "/login/index.php",
    "p50_latency":        "/*",
    "p95_latency":        "/*",
    "p99_latency":        "/*",
    "avg_response_time":  "/*",
    "avg_upstream_time":  "/*",
    "max_endpoint_p95":   "/* (pior endpoint)",
    "rps":                "/*",
    "error_count":        "/*",
    "http_2xx":           "/*",
    "http_3xx":           "/*",
    "http_4xx":           "/*",
    "http_5xx":           "/*",
    "mobile_count":       "/*",
    "desktop_count":      "/*",
    "cpu_avg":            "system",
    "mem_avg":            "system",
}


def enrich(events: List[dict], host: str = "moodle-container") -> List[dict]:
    for event in events:
        metric = event.get("metric_name", "")
        event.setdefault("host", host)
        event.setdefault("endpoint", _METRIC_ENDPOINT.get(metric, "system"))
        event.setdefault("category", _METRIC_CATEGORY.get(metric, "other"))
    return events
