"""
Etapa 5 — Extração de features numéricas por grupo.

Transforma os eventos de cada grupo em um vetor de atributos quantitativos
que o modelo de ML pode analisar.

Features extraídas:
  cpu_avg, mem_avg                        — infraestrutura (Prometheus)
  mysql_down, mysql_qps,
  mysql_threads_running                   — banco de dados (Prometheus)
  p50_latency, p95_latency, p99_latency,
  avg_response_time, max_endpoint_p95     — latência (Loki)
  rps                                     — tráfego (Loki)
  error_count, http_4xx, http_5xx,
  client_aborts                           — erros (Loki)
  login_attempts                          — segurança (Loki)
  mobile_count, desktop_count             — distribuição de dispositivos (Loki)
"""

from typing import Dict, List

FEATURE_NAMES: List[str] = [
    # infraestrutura
    "cpu_avg",
    "mem_avg",
    # banco de dados
    "mysql_down",
    "mysql_qps",
    "mysql_threads_running",
    # latência
    "p50_latency",
    "p95_latency",
    "p99_latency",
    "avg_response_time",
    "max_endpoint_p95",
    # tráfego
    "rps",
    # erros
    "error_count",
    "http_4xx",
    "http_5xx",
    "client_aborts",
    # segurança
    "login_attempts",
    # dispositivos
    "mobile_count",
    "desktop_count",
]


def _last_value(events: List[dict], metric_name: str) -> float:
    """Retorna o valor mais recente da métrica no grupo (ou 0.0)."""
    matching = [
        e["value"] for e in events
        if e.get("metric_name") == metric_name
    ]
    return matching[-1] if matching else 0.0


def extract(group: dict) -> Dict[str, float]:
    events = group["events"]
    features = {name: _last_value(events, name) for name in FEATURE_NAMES}

    # Derive mysql_down from mysql_up (1=up, 0=down → invert so anomaly = high value)
    mysql_up = _last_value(events, "mysql_up")
    features["mysql_down"] = 1.0 - mysql_up

    return features


def to_vector(features: Dict[str, float]) -> List[float]:
    return [features.get(name, 0.0) for name in FEATURE_NAMES]
