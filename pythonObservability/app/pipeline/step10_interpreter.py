"""
Etapa 10 — Interpretação operacional final.

Combina o incidente candidato com a análise semântica do LLM e produz
a saída final do pipeline: estrutura completa pronta para consumo humano.
"""

from typing import List


def interpret(incident: dict, llm_analysis: dict) -> dict:
    return {
        "incident_candidate_id": incident["incident_candidate_id"],
        "main_service": incident["main_service"],
        "related_services": incident.get("related_services", [incident["main_service"]]),
        "inferred_related_services": incident.get("inferred_related_services", []),
        "time_window": incident["time_window"],
        "anomaly_score": incident["anomaly_score"],
        "categories_affected": incident["categories_affected"],
        "evidence": incident["evidence"],
        "semantic_summary": llm_analysis.get("semantic_summary", ""),
        "possible_cause": llm_analysis.get("possible_cause", ""),
        "recommended_checks": llm_analysis.get("recommended_checks", []),
    }


def interpret_all(incidents: List[dict], llm_analyses: List[dict]) -> List[dict]:
    return [
        interpret(incident, analysis)
        for incident, analysis in zip(incidents, llm_analyses)
    ]
