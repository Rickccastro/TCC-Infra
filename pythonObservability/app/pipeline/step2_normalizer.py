"""
Etapa 2 — Normalização dos dados brutos em eventos padronizados.

Transforma o dict {metric_name: {timestamp_s: value}} em uma lista plana
de eventos com schema unificado. Cada ponto de cada série vira um evento.

Schema do evento:
{
    "timestamp": float,
    "service": str,
    "type": "metric",
    "metric_name": str,
    "value": float,
}
"""

from typing import Dict, List


def normalize(raw_series: Dict[str, Dict[int, float]], service: str = "moodle") -> List[dict]:
    events: List[dict] = []

    for metric_name, series in raw_series.items():
        for ts, value in series.items():
            events.append({
                "timestamp": float(ts),
                "service": service,
                "type": "metric",
                "metric_name": metric_name,
                "value": value,
            })

    events.sort(key=lambda e: e["timestamp"])
    return events
