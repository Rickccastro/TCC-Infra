"""
Etapa 6 — Detecção de anomalias com Machine Learning (IsolationForest).

Estado persistido em Parquet (rolling buffer de 14 dias).
Thread-safe via threading.Lock.

Parâmetros padrão:
  contamination = 0.1   → espera 10% de anomalias no histórico
  min_samples   = 10    → treina somente após acumular amostras suficientes
  n_estimators  = 100
  retention_days = 14   → descarta vetores mais velhos que 14 dias
"""

import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from app.pipeline.step5_feature_engineer import FEATURE_NAMES

_PARQUET_COLUMNS = ["timestamp"] + FEATURE_NAMES


class AnomalyDetector:

    def __init__(
        self,
        contamination: float = 0.1,
        min_samples: int = 10,
        n_estimators: int = 100,
        retention_days: int = 14,
        storage_path: str = "/data/feature_history.parquet",
    ):
        self._contamination = contamination
        self._min_samples = min_samples
        self._n_estimators = n_estimators
        self._retention_days = retention_days
        self._storage_path = Path(storage_path)
        self._model: IsolationForest | None = None
        self._lock = threading.Lock()

        self._df = self._load()
        self._rebuild_model()

    # ── persistência ────────────────────────────────────────────────────────

    def _load(self) -> pd.DataFrame:
        if not self._storage_path.exists():
            return pd.DataFrame(columns=_PARQUET_COLUMNS)
        df = pd.read_parquet(self._storage_path)
        return self._prune(df)

    def _prune(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=self._retention_days)
        return df[df["timestamp"] >= cutoff].reset_index(drop=True)

    def _save(self) -> None:
        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._df.to_parquet(self._storage_path, index=False)

    # ── modelo ──────────────────────────────────────────────────────────────

    def _rebuild_model(self) -> None:
        if len(self._df) >= self._min_samples:
            X = self._df[FEATURE_NAMES].to_numpy()
            self._model = IsolationForest(
                contamination=self._contamination,
                n_estimators=self._n_estimators,
                random_state=42,
            )
            self._model.fit(X)

    # ── interface pública ───────────────────────────────────────────────────

    def add_sample(self, vector: List[float]) -> None:
        with self._lock:
            row = {"timestamp": datetime.now(tz=timezone.utc)}
            row.update(dict(zip(FEATURE_NAMES, vector)))
            self._df = pd.concat(
                [self._df, pd.DataFrame([row])], ignore_index=True
            )
            self._df = self._prune(self._df)
            self._save()
            self._rebuild_model()

    def predict(self, vector: List[float]) -> Dict:
        with self._lock:
            if self._model is None:
                return {
                    "anomaly": False,
                    "score": 0.0,
                    "trained": False,
                    "samples_collected": len(self._df),
                }
            x = np.array([vector])
            label = self._model.predict(x)[0]
            score = float(self._model.score_samples(x)[0])
            return {
                "anomaly": bool(label == -1),
                "score": score,
                "trained": True,
                "samples_collected": len(self._df),
            }

    def reset(self) -> None:
        with self._lock:
            self._df = pd.DataFrame(columns=_PARQUET_COLUMNS)
            self._model = None
            if self._storage_path.exists():
                self._storage_path.unlink()

    @property
    def status(self) -> Dict:
        with self._lock:
            oldest = None
            newest = None
            if not self._df.empty:
                oldest = self._df["timestamp"].min().isoformat()
                newest = self._df["timestamp"].max().isoformat()
            return {
                "trained": self._model is not None,
                "samples_collected": len(self._df),
                "min_samples": self._min_samples,
                "contamination": self._contamination,
                "retention_days": self._retention_days,
                "oldest_sample": oldest,
                "newest_sample": newest,
            }
