"""
Etapa 1 — Coleta de dados brutos de observabilidade.

Busca séries temporais do Loki (logs nginx) e Prometheus (CPU, memória)
para o intervalo [start_s, end_s] com granularidade `step` segundos.
Retorna um dict {metric_name: {timestamp_s: value}}.
"""

from typing import Dict

from app.collectors.loki_collector import LokiCollector
from app.collectors.prometheus_collector import PrometheusCollector

# ── Queries LogQL (adaptadas para query_metric_range) ────────────────────────

LOKI_QUERIES: Dict[str, str] = {
    "rps": """
        sum(rate({job="nginx", type="access"}[1m]))
    """,
    "error_count": """
        sum(count_over_time({job="nginx"} |~ " [45][0-9]{2} " [1m]))
    """,
    "p50_latency": """
        sum(
            quantile_over_time(0.50,
                {job="nginx", type="access"}
                | regexp "(?P<request_time>[0-9.]+)$"
                | unwrap request_time [5m]
            )
        )
    """,
    "p95_latency": """
        sum(
            quantile_over_time(0.95,
                {job="nginx", type="access"}
                | regexp "(?P<request_time>[0-9.]+)$"
                | unwrap request_time [5m]
            )
        )
    """,
    "p99_latency": """
        sum(
            quantile_over_time(0.99,
                {job="nginx", type="access"}
                | regexp "(?P<request_time>[0-9.]+)$"
                | unwrap request_time [5m]
            )
        )
    """,
    "avg_response_time": """
        avg(
            avg_over_time(
                {job="nginx", type="access"} != "ELB-HealthChecker"
                | regexp "(?P<request_time>[0-9.]+) [0-9.]+$"
                | unwrap request_time [5m]
            )
        )
    """,
    "avg_upstream_time": """
        avg(
            avg_over_time(
                {job="nginx", type="access"} != "ELB-HealthChecker"
                | regexp "[0-9.]+ (?P<upstream_response_time>[0-9.]+)$"
                | unwrap upstream_response_time [5m]
            )
        )
    """,
    "login_attempts": """
        sum(
            count_over_time(
                {job="nginx"} | json | method="POST" | uri="/login/index.php"
            [1m])
        )
    """,
    "login_avg_response": """
        avg(
            avg_over_time(
                {job="nginx"} | json | uri="/login/index.php" | method="POST"
                | unwrap request_time [1m]
            )
        )
    """,
    "client_aborts": """
        sum(count_over_time({job="nginx"} |= " 499 " [1m]))
    """,
    # ── http_status_distribution (queries.md) ────────────────────────────────
    "http_2xx": """
        sum(count_over_time({job="nginx"} |~ " 2[0-9]{2} " [1m]))
    """,
    "http_3xx": """
        sum(count_over_time({job="nginx"} |~ " 3[0-9]{2} " [1m]))
    """,
    "http_4xx": """
        sum(count_over_time({job="nginx"} |~ " 4[0-9]{2} " [1m]))
    """,
    "http_5xx": """
        sum(count_over_time({job="nginx"} |~ " 5[0-9]{2} " [1m]))
    """,
    # ── top_slowest_endpoints (queries.md) — escalar: pior P95 entre todas URIs
    "max_endpoint_p95": """
        max(
            quantile_over_time(0.95,
                {job="nginx", type="access"}
                | json
                | unwrap request_time [5m]
            ) by (uri)
        )
    """,
    # ── device_distribution (queries.md) — mobile vs desktop
    "mobile_count": """
        sum(count_over_time(
            {job="nginx"} | json
            | line_format `{{.http_user_agent}}`
            !~ `(css|js|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|map)`
            !~ `(?i)bot|crawler|spider`
            | regexp `(?P<device>Android|iPhone|iPad|Mobile|Tablet)`
            | device != ""
        [1m]))
    """,
    "desktop_count": """
        sum(count_over_time(
            {job="nginx"} | json
            | line_format `{{.http_user_agent}}`
            !~ `(css|js|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|map)`
            !~ `(?i)bot|crawler|spider`
            | regexp `(?P<device>Windows NT|Macintosh|Mac OS X|Linux x86_64|Ubuntu|X11|ChromeOS)`
            | device != ""
        [1m]))
    """,
}

# ── Queries PromQL ────────────────────────────────────────────────────────────

PROMETHEUS_QUERIES: Dict[str, str] = {
    "cpu_avg": '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)',
    "mem_avg": (
        "(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)"
        " / node_memory_MemTotal_bytes * 100"
    ),
}

ALL_METRIC_NAMES = list(LOKI_QUERIES.keys()) + list(PROMETHEUS_QUERIES.keys())


def _parse_matrix(raw: dict) -> Dict[int, float]:
    """Converte resposta matrix do Loki/Prometheus em {timestamp_s: value}."""
    series: Dict[int, float] = {}
    for result in raw.get("data", {}).get("result", []):
        for ts, val in result.get("values", []):
            try:
                series[int(float(ts))] = float(val)
            except (ValueError, TypeError):
                continue
    return series


def collect_raw(start_s: int, end_s: int, step: int = 60) -> Dict[str, Dict[int, float]]:
    """
    Retorna {metric_name: {timestamp_s: value}} para todas as métricas
    no intervalo [start_s, end_s].
    """
    all_series: Dict[str, Dict[int, float]] = {}

    for name, query in LOKI_QUERIES.items():
        raw = LokiCollector.query_metric_range(query, start_s, end_s, step)
        all_series[name] = _parse_matrix(raw)

    for name, query in PROMETHEUS_QUERIES.items():
        raw = PrometheusCollector.query_range(query, start_s, end_s, step)
        all_series[name] = _parse_matrix(raw)

    return all_series
