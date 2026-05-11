import logging

from fastapi import FastAPI, Query

from app.services.aiops_service import AIOpsService

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

app = FastAPI(
    title="pythonObservability — AIOps Pipeline",
    description=(
        "Pipeline AIOps com 10 etapas: coleta → normalização → enriquecimento → "
        "agrupamento → features → ML (IsolationForest) → correlação → "
        "incidente candidato → LLM (Claude) → interpretação operacional."
    ),
    version="1.0.0",
)


@app.get("/pipeline/run")
def run_pipeline(
    history_minutes: int = Query(default=60, ge=5, le=1440, description="Minutos de histórico para treinar o modelo"),
    window_seconds: int = Query(default=60, ge=10, le=3600, description="Tamanho da janela de agrupamento em segundos"),
):
    """
    Executa o pipeline AIOps completo.
    Coleta métricas do Loki e Prometheus, detecta anomalias com IsolationForest
    e interpreta o incidente com Claude (LLM).
    """
    return AIOpsService.run_pipeline(
        history_minutes=history_minutes,
        window_seconds=window_seconds,
    )


@app.get("/pipeline/status")
def pipeline_status():
    """Estado atual do modelo de ML (treinado, amostras coletadas)."""
    return AIOpsService.get_status()


@app.post("/pipeline/reset")
def pipeline_reset():
    """Limpa o histórico do modelo e descarta o treinamento atual."""
    return AIOpsService.reset_model()
