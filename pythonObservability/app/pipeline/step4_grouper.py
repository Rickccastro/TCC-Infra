"""
Etapa 4 — Agrupamento por janela temporal e serviço.

Agrupa eventos em janelas de `window_seconds` segundos, dentro do mesmo serviço.
Cada grupo representa um snapshot operacional do sistema naquele período.

Saída:
[
  {
    "group_id": "moodle_1234567800",
    "service": "moodle",
    "window_start": float,
    "window_end": float,
    "events": [...],
  }
]
"""

import math
from collections import defaultdict
from typing import List


def group(events: List[dict], window_seconds: int = 60) -> List[dict]:
    buckets = defaultdict(list)

    for event in events:
        ts = event["timestamp"]
        service = event.get("service", "unknown")
        window_idx = math.floor(ts / window_seconds)
        key = (service, window_idx)
        buckets[key].append(event)

    groups = []
    for (service, window_idx), group_events in sorted(buckets.items()):
        window_start = window_idx * window_seconds
        window_end = window_start + window_seconds
        groups.append({
            "group_id": f"{service}_{int(window_start)}",
            "service": service,
            "window_start": float(window_start),
            "window_end": float(window_end),
            "events": group_events,
        })

    return groups
