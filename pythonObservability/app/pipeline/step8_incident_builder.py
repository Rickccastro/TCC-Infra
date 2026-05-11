"""
Etapa 8 — Construção do incidente candidato.

Organiza os dados de correlação em uma estrutura única que representa
um possível incidente operacional. Essa estrutura é a entrada do LLM.

Saída:
{
  "incident_candidate_id": str,
  "summary": str,
  "main_service": str,
  "related_services": [str],
  "time_window": str,
  "anomaly_score": float,
  "evidence": [str],
  "categories_affected": [str],
}
"""

import hashlib
from typing import List

_SEVERITY_LABEL = {
    "critical": "crítico",
    "warning": "elevado",
}

_METRIC_LABEL = {
    "cpu_avg": "CPU média",
    "mem_avg": "memória média",
    "p50_latency": "latência P50",
    "p95_latency": "latência P95",
    "p99_latency": "latência P99",
    "avg_response_time": "tempo médio de resposta",
    "rps": "requisições por segundo",
    "error_count": "contagem de erros HTTP 4xx/5xx",
    "login_attempts": "tentativas de login",
    "client_aborts": "conexões abortadas (499)",
}


def _incident_id(group_id: str) -> str:
    h = hashlib.md5(group_id.encode()).hexdigest()[:8]
    return f"incident_{h}"


def _build_evidence(signals: List[dict]) -> List[str]:
    evidence = []
    for s in signals:
        label = _METRIC_LABEL.get(s["metric"], s["metric"])
        sev = _SEVERITY_LABEL.get(s["severity"], s["severity"])
        evidence.append(f"{label}: {s['value']} ({sev})")
    return evidence


def build(correlations: List[dict]) -> List[dict]:
    incidents = []

    for corr in correlations:
        evidence = _build_evidence(corr["correlated_signals"])
        categories = corr["categories_affected"]

        inferred = corr.get("inferred_related_services", [])
        related_services = [corr["service"]] + [r["service"] for r in inferred]

        category_summary = ", ".join(categories) if categories else "comportamento geral"
        related_summary = (
            f" Serviços relacionados inferidos: {', '.join(r['service'] for r in inferred)}."
            if inferred else ""
        )
        summary = (
            f"Anomalia detectada no serviço {corr['service']} "
            f"na janela {corr['time_window']}. "
            f"Categorias afetadas: {category_summary}.{related_summary}"
        )

        incidents.append({
            "incident_candidate_id": _incident_id(corr["anomalous_group_id"]),
            "summary": summary,
            "main_service": corr["service"],
            "related_services": related_services,
            "inferred_related_services": inferred,
            "time_window": corr["time_window"],
            "window_start": corr["window_start"],
            "window_end": corr["window_end"],
            "anomaly_score": corr["anomaly_score"],
            "evidence": evidence,
            "categories_affected": categories,
            "correlated_signals": corr["correlated_signals"],
        })

    return incidents
