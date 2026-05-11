"""
Etapa 9 — Análise semântica com LLM (Ollama — modelo local no Docker stack).

Recebe um incidente candidato e retorna uma interpretação operacional
em linguagem natural com hipótese de causa e verificações recomendadas.

O LLM não detecta anomalias — apenas interpreta o que o ML já sinalizou.
O modelo roda localmente via Ollama (llama3.2 por padrão), sem custo e sem
dados saindo da rede. O parâmetro `format: "json"` força saída JSON válido.
Enriquece o prompt com snippets de log bruto coletados do Loki na janela do incidente.
"""

import json
import os

import requests

from app.collectors.loki_collector import LokiCollector

_OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://ollama:11434")
_OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")
_OLLAMA_TIMEOUT = 120  # segundos — modelos locais podem ser lentos na primeira inferência

_SYSTEM_PROMPT = """\
Você é um engenheiro de observabilidade especialista em sistemas Moodle rodando em Docker.
Você analisa incidentes candidatos gerados por um pipeline AIOps que monitora logs nginx, \
métricas de CPU/memória (Prometheus) e logs de aplicação (Loki).

Seu papel é interpretar semanticamente o incidente e responder SEMPRE em JSON válido \
com os campos: semantic_summary, possible_cause e recommended_checks (lista de strings).

Seja objetivo e técnico. Não repita os dados numéricos na explicação — foque no significado operacional.\
"""


def _build_prompt(incident: dict, log_lines: list[str]) -> str:
    evidence_block = "\n".join(f"- {e}" for e in incident.get("evidence", []))
    categories = ", ".join(incident.get("categories_affected", []))

    inferred = incident.get("inferred_related_services", [])
    related_block = ""
    if inferred:
        related_block = "\nServiços relacionados inferidos:\n" + "\n".join(
            f"- {r['service']}: {r['reason']}" for r in inferred
        )

    log_block = ""
    if log_lines:
        log_block = "\nLinhas de log nginx coletadas na janela do incidente:\n" + "\n".join(
            f"  {line}" for line in log_lines
        )

    return f"""\
Analise o seguinte incidente candidato detectado automaticamente.

Serviço principal afetado: {incident["main_service"]}.
Janela temporal: {incident["time_window"]}.
Score de anomalia: {incident["anomaly_score"]:.4f} (quanto mais negativo, mais anômalo).
Categorias de sinais afetados: {categories or "não identificadas"}.
{related_block}
Evidências observadas:
{evidence_block}
{log_block}
Responda em JSON com os campos:
- semantic_summary: resumo do que foi observado (2-3 frases)
- possible_cause: hipótese provável de causa raiz (1-2 frases)
- recommended_checks: lista de 3 a 5 verificações iniciais recomendadas
"""


def analyze(incident: dict) -> dict:
    """
    Envia o incidente candidato ao Ollama e retorna a interpretação semântica.
    Coleta snippets de log bruto do Loki para enriquecer o contexto do prompt.
    Retorna dict com semantic_summary, possible_cause e recommended_checks.
    """
    log_lines = LokiCollector.get_raw_log_lines(
        start_s=int(incident.get("window_start", 0)),
        end_s=int(incident.get("window_end", 0)),
        limit=10,
    )

    full_prompt = _SYSTEM_PROMPT + "\n\n" + _build_prompt(incident, log_lines)

    try:
        response = requests.post(
            f"{_OLLAMA_HOST}/api/generate",
            json={
                "model": _OLLAMA_MODEL,
                "prompt": full_prompt,
                "stream": False,
                "format": "json",
            },
            timeout=_OLLAMA_TIMEOUT,
        )
        response.raise_for_status()

        raw_text = response.json().get("response", "").strip()
        parsed = json.loads(raw_text)

        return {
            "semantic_summary": parsed.get("semantic_summary", ""),
            "possible_cause": parsed.get("possible_cause", ""),
            "recommended_checks": parsed.get("recommended_checks", []),
        }

    except requests.exceptions.ConnectionError:
        return {
            "semantic_summary": "Ollama inacessível — verifique se o serviço está rodando.",
            "possible_cause": "N/A",
            "recommended_checks": [],
        }
    except requests.exceptions.Timeout:
        return {
            "semantic_summary": f"Timeout ao chamar Ollama ({_OLLAMA_TIMEOUT}s). Modelo pode estar carregando.",
            "possible_cause": "N/A",
            "recommended_checks": [],
        }
    except Exception as exc:
        return {
            "semantic_summary": f"Erro ao chamar Ollama: {exc}",
            "possible_cause": "N/A",
            "recommended_checks": [],
        }
