"""
Orquestrador do pipeline AIOps completo (10 etapas).

Instância global do detector persiste entre requests.
"""

import logging
import time
from typing import Dict, List

from app.pipeline import (
    step1_collector,
    step2_normalizer,
    step3_enricher,
    step4_grouper,
    step5_feature_engineer,
    step6_anomaly_detector,
    step7_correlator,
    step8_incident_builder,
    step9_llm_analyzer,
    step10_interpreter,
)

_detector = step6_anomaly_detector.AnomalyDetector(contamination=0.1, min_samples=10)
_log = logging.getLogger("aiops")


class AIOpsService:

    @staticmethod
    def run_pipeline(history_minutes: int = 60, window_seconds: int = 60) -> Dict:
        t0 = time.time()
        now_s = int(t0)
        start_s = now_s - history_minutes * 60
        _log.info("[pipeline] inicio history=%dm window=%ds", history_minutes, window_seconds)

        # ── Etapa 1: coleta ──────────────────────────────────────────────────
        _log.info("[step1] coletando series do Loki e Prometheus...")
        raw_series = step1_collector.collect_raw(start_s, now_s, step=window_seconds)
        _log.info("[step1] concluido em %.1fs — %d metricas", time.time() - t0, len(raw_series))
        if not raw_series:
            return {"error": "Nenhuma série coletada do Loki ou Prometheus"}

        # ── Etapa 2: normalização ────────────────────────────────────────────
        _log.info("[step2] normalizando eventos...")
        events = step2_normalizer.normalize(raw_series)
        _log.info("[step2] %d eventos normalizados", len(events))
        if not events:
            return {"error": "Nenhum evento após normalização"}

        # ── Etapa 3: enriquecimento ──────────────────────────────────────────
        _log.info("[step3] enriquecendo eventos...")
        events = step3_enricher.enrich(events)

        # ── Etapa 4: agrupamento ─────────────────────────────────────────────
        _log.info("[step4] agrupando por janela de %ds...", window_seconds)
        groups = step4_grouper.group(events, window_seconds=window_seconds)
        _log.info("[step4] %d grupos formados", len(groups))
        if len(groups) < 2:
            return {"error": "Dados insuficientes para separar histórico e janela atual"}

        # ── Etapa 5 + 6: features e detecção ML ─────────────────────────────
        _log.info("[step5/6] extraindo features e detectando anomalias...")
        annotated_groups: List[dict] = []
        for i, grp in enumerate(groups):
            features = step5_feature_engineer.extract(grp)
            vector = step5_feature_engineer.to_vector(features)

            if i < len(groups) - 1:
                _detector.add_sample(vector)

            prediction = _detector.predict(vector)

            annotated_groups.append({
                **grp,
                "features": features,
                "prediction": prediction,
            })

        # ── Etapa 7: correlação de eventos ───────────────────────────────────
        _log.info("[step7] correlacionando sinais anomalos...")
        correlations = step7_correlator.correlate(annotated_groups)

        if not correlations:
            _log.info("[pipeline] sem anomalia detectada — concluido em %.1fs", time.time() - t0)
            latest = annotated_groups[-1]
            return {
                "anomaly_detected": False,
                "model_status": _detector.status,
                "latest_group": {
                    "group_id": latest["group_id"],
                    "time_window": f"{latest['window_start']} - {latest['window_end']}",
                    "features": latest["features"],
                    "prediction": latest["prediction"],
                },
            }

        # ── Etapa 8: incidentes candidatos ───────────────────────────────────
        _log.info("[step8] construindo %d incidente(s) candidato(s)...", len(correlations))
        incidents = step8_incident_builder.build(correlations)

        # ── Etapa 9: análise LLM — limita ao top 3 mais anômalos ─────────────
        incidents_for_llm = sorted(incidents, key=lambda x: x.get("anomaly_score", 0))[:3]
        _log.info("[step9] chamando LLM para %d/%d incidente(s)...", len(incidents_for_llm), len(incidents))
        t_llm = time.time()
        llm_analyses = [step9_llm_analyzer.analyze(inc) for inc in incidents_for_llm]
        _log.info("[step9] LLM concluido em %.1fs", time.time() - t_llm)
        incidents = incidents_for_llm

        # ── Etapa 10: interpretação operacional ──────────────────────────────
        _log.info("[step10] interpretando resultado final...")
        results = step10_interpreter.interpret_all(incidents, llm_analyses)

        _log.info("[pipeline] concluido em %.1fs — %d incidente(s)", time.time() - t0, len(results))
        return {
            "anomaly_detected": True,
            "model_status": _detector.status,
            "incidents": results,
        }

    @staticmethod
    def get_status() -> Dict:
        return _detector.status

    @staticmethod
    def reset_model() -> Dict:
        _detector.reset()
        return {"status": "model reset"}
