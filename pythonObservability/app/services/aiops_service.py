"""
Orquestrador do pipeline AIOps completo (10 etapas).

Instância global do detector persiste entre requests.
"""

import json
import os
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
_LLM_ENABLED: bool = os.getenv("LLM_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
_log = logging.getLogger("aiops")

# ── helpers de formatação ─────────────────────────────────────────────────────

def _sep(char: str = "─", width: int = 70) -> str:
    return char * width

def _json(obj, indent: int = 2) -> str:
    """Serializa obj para JSON legível, tolerando tipos não-serializáveis."""
    def _default(o):
        try:
            return float(o)
        except Exception:
            return str(o)
    return json.dumps(obj, indent=indent, ensure_ascii=False, default=_default)


# ── sub-rotinas de log por módulo ─────────────────────────────────────────────

def _log_module1(raw_series: Dict, events: List[dict]) -> None:
    """
    MÓDULO 1 — Coleta · Normalização · Enriquecimento
    Mostra:
      - quantas séries foram coletadas e quantos pontos cada uma tem
      - distribuição de categorias de eventos após enriquecimento
      - amostra dos 5 eventos mais recentes (para validação)
    """
    _log.info(_sep("═"))
    _log.info("MÓDULO 1 — COLETA, NORMALIZAÇÃO E ENRIQUECIMENTO")
    _log.info(_sep("═"))

    # ── Step 1: séries brutas coletadas ──────────────────────────────────────
    _log.info("[step1] %d séries coletadas do Loki + Prometheus:", len(raw_series))
    for name, series in sorted(raw_series.items()):
        n_points = len(series)
        if n_points == 0:
            _log.info("  %-30s  0 pontos  ⚠ SEM DADOS", name)
        else:
            values = list(series.values())
            _log.info(
                "  %-30s  %3d pontos  min=%.4f  max=%.4f  último=%.4f",
                name, n_points, min(values), max(values), values[-1],
            )

    # métricas com zero pontos (dados ausentes = problema de coleta)
    empty = [n for n, s in raw_series.items() if not s]
    if empty:
        _log.warning("[step1] MÉTRICAS SEM DADOS: %s", ", ".join(empty))
    else:
        _log.info("[step1] Todas as métricas retornaram dados")

    # ── Step 2 + 3: eventos normalizados e enriquecidos ───────────────────────
    _log.info(_sep())
    _log.info("[step2/3] %d eventos normalizados e enriquecidos", len(events))

    # distribuição por categoria
    from collections import Counter
    cat_counts = Counter(e.get("category", "other") for e in events)
    metric_counts = Counter(e["metric_name"] for e in events)

    _log.info("[step3] Distribuição por categoria:")
    for cat, cnt in sorted(cat_counts.items()):
        _log.info("  %-20s  %d eventos", cat, cnt)

    _log.info("[step3] Distribuição por métrica:")
    for metric, cnt in sorted(metric_counts.items()):
        _log.info("  %-30s  %d eventos", metric, cnt)

    # amostra dos 5 eventos mais recentes
    recent = sorted(events, key=lambda e: e["timestamp"], reverse=True)[:5]
    _log.info("[step3] Amostra dos 5 eventos mais recentes:")
    for ev in recent:
        import datetime
        ts_str = datetime.datetime.utcfromtimestamp(ev["timestamp"]).strftime("%H:%M:%S")
        _log.info(
            "  [%s UTC] %-30s = %-12.4f  cat=%-15s  endpoint=%s",
            ts_str,
            ev["metric_name"],
            ev["value"],
            ev.get("category", "?"),
            ev.get("endpoint", "?"),
        )
    _log.info(_sep("═"))


def _log_module2(groups: List[dict], annotated_groups: List[dict], correlations: List[dict]) -> None:
    """
    MÓDULO 2 — Agrupamento · Feature Engineering · ML · Correlação
    Mostra:
      - cada janela temporal formada com contagem de eventos
      - vetor de features de cada grupo
      - resultado da predição ML (score + anomaly flag)
      - correlações encontradas com sinais e serviços inferidos
    """
    _log.info(_sep("═"))
    _log.info("MÓDULO 2 — AGRUPAMENTO, FEATURES, ML E CORRELAÇÃO")
    _log.info(_sep("═"))

    # ── Step 4: grupos / janelas temporais ────────────────────────────────────
    _log.info("[step4] %d janelas temporais formadas:", len(groups))
    for g in groups:
        import datetime
        t_start = datetime.datetime.utcfromtimestamp(g["window_start"]).strftime("%H:%M:%S")
        t_end   = datetime.datetime.utcfromtimestamp(g["window_end"]).strftime("%H:%M:%S")
        _log.info(
            "  [%s - %s UTC]  group_id=%-30s  eventos=%d",
            t_start, t_end, g["group_id"], len(g["events"]),
        )

    # ── Step 5 + 6: features e predição ML por grupo ──────────────────────────
    _log.info(_sep())
    _log.info("[step5/6] Vetores de features e predições ML por janela:")

    n_anomalous = 0
    for ag in annotated_groups:
        import datetime
        t_start = datetime.datetime.utcfromtimestamp(ag["window_start"]).strftime("%H:%M:%S")
        pred    = ag.get("prediction", {})
        is_anom = pred.get("anomaly", False)
        score   = pred.get("score", 0.0)
        trained = pred.get("trained", False)
        n_samples = pred.get("samples_collected", 0)

        anomaly_marker = "ANOMALIA" if is_anom else "✓ normal"
        model_marker   = f"[modelo treinado | {n_samples} amostras]" if trained else "[modelo NÃO treinado ainda]"

        _log.info(
            "  [%s UTC]  %s  score=%.4f  %s",
            t_start, anomaly_marker, score, model_marker,
        )

        # features do grupo (todas, com destaque para as não-zero)
        features = ag.get("features", {})
        non_zero = {k: v for k, v in features.items() if v != 0.0}
        zero_keys = [k for k, v in features.items() if v == 0.0]

        if non_zero:
            _log.info("    Features com valor > 0:")
            for feat, val in sorted(non_zero.items()):
                _log.info("      %-30s = %.4f", feat, val)
        if zero_keys:
            _log.info("    Features zeradas: %s", ", ".join(sorted(zero_keys)))

        if is_anom:
            n_anomalous += 1

    _log.info(
        "[step6] Resumo ML: %d/%d janelas anômalas | modelo_treinado=%s",
        n_anomalous,
        len(annotated_groups),
        annotated_groups[-1].get("prediction", {}).get("trained", False) if annotated_groups else False,
    )

    # ── Step 7: correlações ───────────────────────────────────────────────────
    _log.info(_sep())
    if not correlations:
        _log.info("[step7] Nenhuma correlação encontrada — sistema operando normalmente")
    else:
        _log.info("[step7] %d correlação(ões) detectada(s):", len(correlations))
        for i, corr in enumerate(correlations, 1):
            _log.info("  Correlação #%d  group=%s  janela=%s  score=%.4f",
                      i, corr["anomalous_group_id"], corr["time_window"], corr["anomaly_score"])

            # sinais correlacionados
            signals = corr.get("correlated_signals", [])
            if signals:
                _log.info("    Sinais acima do threshold (%d):", len(signals))
                for sig in signals:
                    _log.info(
                        "      %-30s = %-12.4f  cat=%-15s  severity=%s",
                        sig["metric"], sig["value"], sig["category"], sig["severity"],
                    )
            else:
                _log.info("    Nenhum sinal acima do threshold")

            # categorias afetadas
            cats = corr.get("categories_affected", [])
            _log.info("    Categorias afetadas: %s", ", ".join(cats) if cats else "nenhuma")

            # serviços inferidos
            inferred = corr.get("inferred_related_services", [])
            if inferred:
                _log.info("    Serviços relacionados inferidos:")
                for inf in inferred:
                    _log.info("      → %s: %s", inf["service"], inf["reason"])
            else:
                _log.info("    Nenhum serviço relacionado inferido")

    _log.info(_sep("═"))


def _log_module3_input(incidents: List[dict], llm_analyses: List[dict]) -> None:
    """
    MÓDULO 3 — Input que o LLM recebe (step9) para cada incidente.
    Loga o payload exato enviado ao LLM antes da análise.
    """
    _log.info(_sep("═"))
    _log.info("MÓDULO 3 — INPUT DO LLM (step9)")
    _log.info(_sep("═"))
    for i, inc in enumerate(incidents, 1):
        _log.info("  Incidente #%d enviado ao LLM:", i)
        _log.info("  %s", _json(inc))
    _log.info(_sep("═"))


# ── serviço principal ─────────────────────────────────────────────────────────

class AIOpsService:

    @staticmethod
    def run_pipeline(history_minutes: int = 60, window_seconds: int = 60) -> Dict:
        t0 = time.time()
        now_s = int(t0)
        start_s = now_s - history_minutes * 60
        _log.info(_sep("═"))
        _log.info("PIPELINE AIOPS INICIADO  history=%dm  window=%ds", history_minutes, window_seconds)
        _log.info(_sep("═"))

        # ── Etapa 1: coleta ──────────────────────────────────────────────────
        _log.info("[step1] Coletando séries do Loki e Prometheus...")
        t1 = time.time()
        raw_series = step1_collector.collect_raw(start_s, now_s, step=window_seconds)
        _log.info("[step1] Concluído em %.1fs — %d séries recebidas", time.time() - t1, len(raw_series))

        if not raw_series:
            _log.error("[step1] Nenhuma série coletada — abortando pipeline")
            return {"error": "Nenhuma série coletada do Loki ou Prometheus"}

        # ── Etapa 2: normalização ────────────────────────────────────────────
        _log.info("[step2] Normalizando eventos...")
        events = step2_normalizer.normalize(raw_series)
        _log.info("[step2] %d eventos gerados", len(events))

        if not events:
            _log.error("[step2] Nenhum evento após normalização — abortando pipeline")
            return {"error": "Nenhum evento após normalização"}

        # ── Etapa 3: enriquecimento ──────────────────────────────────────────
        _log.info("[step3] Enriquecendo eventos com metadados...")
        events = step3_enricher.enrich(events)
        _log.info("[step3] Enriquecimento concluído")

        # ── LOG MÓDULO 1 ─────────────────────────────────────────────────────
        _log_module1(raw_series, events)

        # ── Etapa 4: agrupamento ─────────────────────────────────────────────
        _log.info("[step4] Agrupando eventos em janelas de %ds...", window_seconds)
        groups = step4_grouper.group(events, window_seconds=window_seconds)
        _log.info("[step4] %d grupos formados", len(groups))

        if len(groups) < 2:
            _log.error("[step4] Dados insuficientes (< 2 grupos) — abortando pipeline")
            return {"error": "Dados insuficientes para separar histórico e janela atual"}

        # ── Etapa 5 + 6: features e detecção ML ─────────────────────────────
        _log.info("[step5/6] Extraindo features e detectando anomalias...")
        annotated_groups: List[dict] = []
        for i, grp in enumerate(groups):
            features = step5_feature_engineer.extract(grp)
            vector   = step5_feature_engineer.to_vector(features)

            if i < len(groups) - 1:
                _detector.add_sample(vector)

            prediction = _detector.predict(vector)

            annotated_groups.append({
                **grp,
                "features": features,
                "prediction": prediction,
            })

        # ── Etapa 7: correlação de eventos ───────────────────────────────────
        _log.info("[step7] Correlacionando sinais anômalos...")
        correlations = step7_correlator.correlate(annotated_groups)
        _log.info("[step7] %d correlação(ões) encontrada(s)", len(correlations))

        # ── LOG MÓDULO 2 ─────────────────────────────────────────────────────
        _log_module2(groups, annotated_groups, correlations)

        # ── sem anomalia ─────────────────────────────────────────────────────
        if not correlations:
            _log.info("Pipeline concluído sem anomalias em %.1fs", time.time() - t0)
            latest = annotated_groups[-1]
            return {
                "anomaly_detected": False,
                "model_status": _detector.status,
                "latest_group": {
                    "group_id":    latest["group_id"],
                    "time_window": f"{latest['window_start']} - {latest['window_end']}",
                    "features":    latest["features"],
                    "prediction":  latest["prediction"],
                },
            }

        # ── Etapa 8: incidentes candidatos ───────────────────────────────────
        _log.info("[step8] Construindo %d incidente(s) candidato(s)...", len(correlations))
        incidents = step8_incident_builder.build(correlations)

        # ── LOG MÓDULO 3: input do LLM ────────────────────────────────────────
        incidents_for_llm = sorted(incidents, key=lambda x: x.get("anomaly_score", 0))[:3]
        _log_module3_input(incidents_for_llm, [])

        # ── Etapa 9: análise LLM (controlada por LLM_ENABLED) ────────────────
        if _LLM_ENABLED:
            _log.info("[step9] LLM_ENABLED=true — chamando LLM para %d/%d incidente(s)...",
                      len(incidents_for_llm), len(incidents))
            t_llm = time.time()
            llm_analyses = [step9_llm_analyzer.analyze(inc) for inc in incidents_for_llm]
            _log.info("[step9] LLM concluído em %.1fs", time.time() - t_llm)
        else:
            _log.info("[step9] LLM_ENABLED=false — etapa LLM ignorada, retornando incidentes sem análise")
            llm_analyses = [{"llm_skipped": True, "reason": "LLM_ENABLED=false"} for _ in incidents_for_llm]

        incidents = incidents_for_llm

        # ── Etapa 10: interpretação operacional ──────────────────────────────
        _log.info("[step10] Interpretando resultado final...")
        results = step10_interpreter.interpret_all(incidents, llm_analyses)

        _log.info("Pipeline concluído em %.1fs — %d incidente(s)", time.time() - t0, len(results))
        return {
            "anomaly_detected": True,
            "model_status": _detector.status,
            "llm_enabled": _LLM_ENABLED,
            "incidents": results,
        }

    @staticmethod
    def get_status() -> Dict:
        return _detector.status

    @staticmethod
    def reset_model() -> Dict:
        _detector.reset()
        return {"status": "model reset"}