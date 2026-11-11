"""Inferencia: carga el Pipeline entrenado y predice el puntaje de un evento.

El modelo guardado (models/model.pkl) incluye su preprocesamiento, así que aquí
solo se arma la fila con las columnas que el Pipeline aprendió, en el mismo
orden del entrenamiento. El puntaje real nunca entra: no es una de esas columnas.
"""
import hashlib
import math
from pathlib import Path
from typing import Any, Mapping

import joblib
import pandas as pd

from happiness_etl.config import MODEL_FILE


class Predictor:
    """Modelo cargado en memoria, listo para predecir evento por evento."""

    def __init__(self, path: Path = MODEL_FILE):
        content = Path(path).read_bytes()
        self.model = joblib.load(path)
        # Columnas y orden exactos con los que se entrenó el Pipeline
        self.columns = list(self.model.feature_names_in_)
        # Identifica el modelo exacto: tipo + huella del archivo. Si se reentrena, cambia sola.
        estimator = self.model.steps[-1][1] if hasattr(self.model, "steps") else self.model
        self.version = f"{type(estimator).__name__}-{hashlib.sha256(content).hexdigest()[:12]}"

    def predict(self, event: Mapping[str, Any]) -> float:
        """Predice el puntaje de un evento ya validado. Lanza una excepción si el modelo no puede."""
        row = pd.DataFrame([{column: event[column] for column in self.columns}])
        prediction = float(self.model.predict(row)[0])
        if not math.isfinite(prediction):
            raise ValueError(f"el modelo devolvió un valor no finito ({prediction})")
        return prediction
