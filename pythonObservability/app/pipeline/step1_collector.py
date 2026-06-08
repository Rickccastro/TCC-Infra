"""
Etapa 1 — Coleta de dados brutos de observabilidade.

Busca séries temporais do Loki (logs nginx) e Prometheus (CPU, memória)
para o intervalo [start_s, end_s] com granularidade `step` segundos.
Retorna um dict {metric_name: {timestamp_s: value}}.

v2 fixes:
  - All Loki queries now use `| json` to parse the JSON log format.
  - Removed all `regexp "...$"` patterns that matched against plaintext
    but never matched JSON (which ends with `}` not a number).
  - Removed all `|~ " NNN "` patterns that relied on spaces around the
    status code — JSON has `"status":200` with no surrounding spaces.
  - Status filtering now uses `| status =~ "Nxx"` after `| json`.
  - type="access" selector added where missing to avoid hitting error.log.
"""

from typing import Dict

from app.collectors.loki_collector import LokiCollector
from app.collectors.prometheus_collector import PrometheusCollector

# ── Queries LogQL ─────────────────────────────────────────────────────────────

LOKI_QUERIES: Dict[str, str] = {

    # Request rate (requests per second over 1-minute window)
    "rps": """
        sum(
            rate(
                {job="nginx", type="access"}
                | json
                [1m]
            )
        )
    """,

    # Total error count (4xx + 5xx) per minute
    # Uses | json then label-filter on the parsed `status` field.
    # status is a string label after | json, so use =~ for numeric range.
    "error_count": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | status =~ "4[0-9]{2}|5[0-9]{2}"
                [1m]
            )
        )
    """,

    # ── Latency percentiles ──────────────────────────────────────────────────
    # All three use | json to extract request_time from the JSON field.
    "p50_latency": """
        sum(
            quantile_over_time(0.50,
                {job="nginx", type="access"}
                | json
                | unwrap request_time [5m]
            )
        )
    """,

    "p95_latency": """
        sum(
            quantile_over_time(0.95,
                {job="nginx", type="access"}
                | json
                | unwrap request_time [5m]
            )
        )
    """,

    "p99_latency": """
        sum(
            quantile_over_time(0.99,
                {job="nginx", type="access"}
                | json
                | unwrap request_time [5m]
            )
        )
    """,

    # Average request time (excludes health-checker traffic)
    "avg_response_time": """
        avg(
            avg_over_time(
                {job="nginx", type="access"}
                | json
                | http_user_agent != "ELB-HealthChecker"
                | unwrap request_time [5m]
            )
        )
    """,

    # Average upstream response time
    "avg_upstream_time": """
        avg(
            avg_over_time(
                {job="nginx", type="access"}
                | json
                | http_user_agent != "ELB-HealthChecker"
                | unwrap upstream_response_time [5m]
            )
        )
    """,

    # ── Login endpoint ────────────────────────────────────────────────────────
    # These were already correct (used | json); kept as-is.
    "login_attempts": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | method = "POST"
                | uri = "/login/index.php"
                [1m]
            )
        )
    """,

    "login_avg_response": """
        avg(
            avg_over_time(
                {job="nginx", type="access"}
                | json
                | uri = "/login/index.php"
                | method = "POST"
                | unwrap request_time [1m]
            )
        )
    """,

    # ── Client aborts (499) ───────────────────────────────────────────────────
    # OLD: |= " 499 "  — spaces don't exist around status in JSON format.
    # NEW: | json | status = "499"
    "client_aborts": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | status = "499"
                [1m]
            )
        )
    """,

    # ── HTTP status distribution ──────────────────────────────────────────────
    # OLD: |~ " 2[0-9]{2} "  — matched against plaintext Apache-style logs.
    # NEW: | json | status =~ "2[0-9]{2}"  — filters the parsed status field.
    # Note: after | json, `status` is a string label (e.g. "200"), not an int.
    "http_2xx": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | status =~ "2[0-9]{2}"
                [1m]
            )
        )
    """,

    "http_3xx": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | status =~ "3[0-9]{2}"
                [1m]
            )
        )
    """,

    "http_4xx": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | status =~ "4[0-9]{2}"
                [1m]
            )
        )
    """,

    "http_5xx": """
        sum(
            count_over_time(
                {job="nginx", type="access"}
                | json
                | status =~ "5[0-9]{2}"
                [1m]
            )
        )
    """,

    # ── Worst P95 latency across all URIs ─────────────────────────────────────
    # Was already correct; kept as-is.
    "max_endpoint_p95": """
        max(
            quantile_over_time(0.95,
                {job="nginx", type="access"}
                | json
                | unwrap request_time [5m]
            ) by (uri)
        )
    """,

    # ── Device distribution ───────────────────────────────────────────────────
    # These use | json + line_format to expose http_user_agent as the log line,
    # then regexp to extract the device type. Logic is preserved; only added
    # type="access" to avoid hitting error.log entries.
    "mobile_count": """
        sum(count_over_time(
            {job="nginx", type="access"}
            | json
            | line_format `{{.http_user_agent}}`
            !~ `(css|js|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|map)`
            !~ `(?i)bot|crawler|spider`
            | regexp `(?P<device>Android|iPhone|iPad|Mobile|Tablet)`
            | device != ""
            [1m]
        ))
    """,

    "desktop_count": """
        sum(count_over_time(
            {job="nginx", type="access"}
            | json
            | line_format `{{.http_user_agent}}`
            !~ `(css|js|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|map)`
            !~ `(?i)bot|crawler|spider`
            | regexp `(?P<device>Windows NT|Macintosh|Mac OS X|Linux x86_64|Ubuntu|X11|ChromeOS)`
            | device != ""
            [1m]
        ))
    """,
}

# ── Queries PromQL ────────────────────────────────────────────────────────────

PROMETHEUS_QUERIES: Dict[str, str] = {
    "cpu_avg": '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)',
    "mem_avg": (
        "(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)"
        " / node_memory_MemTotal_bytes * 100"
    ),

        # ── MySQL Exporter ────────────────────────────────────────────────────────
    "mysql_up": "mysql_up",
    "mysql_qps": "rate(mysql_global_status_queries[1m])",
    "mysql_threads_running": "mysql_global_status_threads_running",
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