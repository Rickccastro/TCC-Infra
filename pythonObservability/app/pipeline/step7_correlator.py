"""
Etapa 7 — Correlação de eventos.

Recebe os grupos anotados com predição ML e busca relacionar a anomalia
com outros sinais observados no mesmo período ou adjacente.

Critérios de correlação:
  - Threshold: métricas que ultrapassaram limites operacionais
  - Heurística cross-service: combinações de features que indicam dependência
    entre serviços (ex: erros + latência elevada → moodledb provavelmente envolvido)

Saída por anomalia detectada:
{
  "anomalous_group_id": str,
  "service": str,
  "time_window": str,
  "anomaly_score": float,
  "correlated_signals": [
    {"metric": str, "value": float, "category": str, "severity": str}
  ],
  "categories_affected": [str],
  "inferred_related_services": [{"service": str, "reason": str}],
}
"""

import datetime
from typing import List

from app.pipeline.step5_feature_engineer import FEATURE_NAMES

_THRESHOLDS = {
    "cpu_avg":            {"warning": 70.0,   "critical": 90.0},
    "mem_avg":            {"warning": 75.0,   "critical": 90.0},
    "p50_latency":        {"warning": 0.5,    "critical": 1.0},
    "p95_latency":        {"warning": 1.0,    "critical": 2.0},
    "p99_latency":        {"warning": 2.0,    "critical": 5.0},
    "avg_response_time":  {"warning": 0.5,    "critical": 1.0},
    "max_endpoint_p95":   {"warning": 2.0,    "critical": 5.0},
    "rps":                {"warning": 50.0,   "critical": 100.0},
    "error_count":        {"warning": 10.0,   "critical": 50.0},
    "http_4xx":           {"warning": 20.0,   "critical": 100.0},
    "http_5xx":           {"warning": 5.0,    "critical": 20.0},
    "client_aborts":      {"warning": 10.0,   "critical": 50.0},
    "login_attempts":     {"warning": 100.0,  "critical": 300.0},
    "mobile_count":       {"warning": 500.0,  "critical": 2000.0},
    "desktop_count":      {"warning": 500.0,  "critical": 2000.0},
}

_FEATURE_CATEGORY = {
    "cpu_avg": "infrastructure",
    "mem_avg": "infrastructure",
    "p50_latency": "latency",
    "p95_latency": "latency",
    "p99_latency": "latency",
    "avg_response_time": "latency",
    "max_endpoint_p95": "latency",
    "rps": "traffic",
    "error_count": "errors",
    "http_4xx": "errors",
    "http_5xx": "errors",
    "client_aborts": "errors",
    "login_attempts": "security",
    "mobile_count": "traffic",
    "desktop_count": "traffic",
}

# Regras heurísticas para inferir serviços relacionados a partir das features.
# Cada regra define uma condição sobre o dict de features e, quando satisfeita,
# adiciona o serviço inferido com o motivo à lista de related_services.
_DEPENDENCY_RULES = [
    {
        "service": "moodledb",
        "condition": lambda f: (
            f.get("error_count", 0) > _THRESHOLDS["error_count"]["warning"]
            and f.get("avg_response_time", 0) > _THRESHOLDS["avg_response_time"]["warning"]
        ),
        "reason": "erros HTTP combinados com latência elevada sugerem degradação no banco de dados",
    },
    {
        "service": "moodledb",
        "condition": lambda f: f.get("http_5xx", 0) > _THRESHOLDS["http_5xx"]["warning"],
        "reason": "erros 5xx em Moodle geralmente indicam falha de backend (DB ou PHP-FPM)",
    },
    {
        "service": "moodledb",
        "condition": lambda f: (
            f.get("p95_latency", 0) > _THRESHOLDS["p95_latency"]["critical"]
            and f.get("cpu_avg", 0) < _THRESHOLDS["cpu_avg"]["warning"]
        ),
        "reason": "latência P95 crítica com CPU normal aponta para gargalo externo (banco de dados ou rede)",
    },
    {
        "service": "auth",
        "condition": lambda f: f.get("login_attempts", 0) > _THRESHOLDS["login_attempts"]["critical"],
        "reason": "volume crítico de tentativas de login pode indicar ataque de força bruta",
    },
    {
        "service": "nginx",
        "condition": lambda f: f.get("client_aborts", 0) > _THRESHOLDS["client_aborts"]["critical"],
        "reason": "volume crítico de conexões abortadas (499) sugere timeouts no proxy nginx",
    },
]


def _severity(metric: str, value: float) -> str:
    thresholds = _THRESHOLDS.get(metric)
    if not thresholds:
        return "normal"
    if value >= thresholds["critical"]:
        return "critical"
    if value >= thresholds["warning"]:
        return "warning"
    return "normal"


def _fmt_window(start: float, end: float) -> str:
    s = datetime.datetime.utcfromtimestamp(start).strftime("%H:%M:%S")
    e = datetime.datetime.utcfromtimestamp(end).strftime("%H:%M:%S")
    return f"{s} - {e} UTC"


def _infer_related_services(features: dict) -> List[dict]:
    """Aplica as regras heurísticas e retorna serviços inferidos sem duplicatas."""
    seen = set()
    related = []
    for rule in _DEPENDENCY_RULES:
        try:
            if rule["condition"](features) and rule["service"] not in seen:
                seen.add(rule["service"])
                related.append({"service": rule["service"], "reason": rule["reason"]})
        except Exception:
            continue
    return related


def correlate(annotated_groups: List[dict]) -> List[dict]:
    """
    annotated_groups: saída do step6 — grupos com chave `prediction` e `features`.
    Retorna lista de correlações apenas para grupos anômalos.
    """
    anomalous = [g for g in annotated_groups if g.get("prediction", {}).get("anomaly")]
    correlations = []

    for group in anomalous:
        features = group.get("features", {})
        signals = []

        for metric in FEATURE_NAMES:
            value = features.get(metric, 0.0)
            severity = _severity(metric, value)
            if severity != "normal":
                signals.append({
                    "metric": metric,
                    "value": round(value, 4),
                    "category": _FEATURE_CATEGORY.get(metric, "other"),
                    "severity": severity,
                })

        categories = list({s["category"] for s in signals})
        inferred = _infer_related_services(features)

        correlations.append({
            "anomalous_group_id": group["group_id"],
            "service": group["service"],
            "time_window": _fmt_window(group["window_start"], group["window_end"]),
            "window_start": group["window_start"],
            "window_end": group["window_end"],
            "anomaly_score": group["prediction"]["score"],
            "correlated_signals": sorted(signals, key=lambda s: s["severity"] == "critical", reverse=True),
            "categories_affected": categories,
            "inferred_related_services": inferred,
        })

    return correlations
