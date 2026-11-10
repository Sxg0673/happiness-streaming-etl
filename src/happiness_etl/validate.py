"""Validate: compuerta de calidad del dataset unificado (Parte A).

Dos momentos:
  1. validate_dataset: antes de cargar, revisa las reglas del esquema unificado.
  2. validate_load: después de cargar, reconcilia el archivo escrito con lo que
     había en memoria (mismas filas, columnas, tipos y valores).

Si alguna regla falla se juntan TODAS las fallas y se lanza ValidationError:
el pipeline se detiene antes de escribir (o avisa que lo escrito no coincide).
Las reglas están escritas de forma declarativa (nombre + chequeo) para poder
llevarlas a Great Expectations más adelante si hace falta.
La validación del contrato del evento (Parte B) se agregará en la Fase 6.
"""
import logging
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from happiness_etl.config import COUNTRY, FEATURES, TARGET, UNIFIED_COLUMNS, YEAR, YEARS

logger = logging.getLogger(__name__)

SCORE_RANGE = (0.0, 10.0)


class ValidationError(Exception):
    """Una o más reglas de calidad fallaron."""

    def __init__(self, stage: str, failures: list[str]):
        self.stage = stage
        self.failures = failures
        detail = "\n".join(f"  - {f}" for f in failures)
        super().__init__(f"Validación '{stage}' falló ({len(failures)} reglas):\n{detail}")


@dataclass(frozen=True)
class Rule:
    """Regla de calidad: el chequeo devuelve cuántas filas o celdas la incumplen."""

    name: str
    count_failures: Callable[[pd.DataFrame], int]


DATASET_RULES = (
    Rule("sin filas", lambda df: int(len(df) == 0)),
    Rule("celdas nulas", lambda df: int(df.isna().sum().sum())),
    Rule("country vacío", lambda df: int((df[COUNTRY].astype(str).str.strip() == "").sum())),
    Rule(f"year fuera de {YEARS[0]}-{YEARS[-1]}", lambda df: int((~df[YEAR].isin(YEARS)).sum())),
    Rule("variables negativas", lambda df: int((df[list(FEATURES)] < 0).sum().sum())),
    Rule(
        f"{TARGET} fuera de {SCORE_RANGE[0]}-{SCORE_RANGE[1]}",
        lambda df: int((~df[TARGET].between(*SCORE_RANGE)).sum()),
    ),
    Rule("par (country, year) repetido", lambda df: int(df.duplicated([COUNTRY, YEAR]).sum())),
)


def check_dataset(df: pd.DataFrame) -> list[str]:
    """Devuelve la lista de fallas del dataset (vacía si todo está bien)."""
    if list(df.columns) != list(UNIFIED_COLUMNS):
        # Sin el esquema correcto las demás reglas no se pueden evaluar
        return [f"columnas {list(df.columns)} distintas de las esperadas {list(UNIFIED_COLUMNS)}"]
    failures = []
    for rule in DATASET_RULES:
        n = rule.count_failures(df)
        if n:
            failures.append(f"{rule.name}: {n}")
    return failures


def validate_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Compuerta antes de cargar: lanza ValidationError si alguna regla falla."""
    failures = check_dataset(df)
    if failures:
        raise ValidationError("dataset", failures)
    logger.info("Validación del dataset OK: %d filas, %d reglas", len(df), len(DATASET_RULES) + 1)
    return df


def validate_load(expected: pd.DataFrame, loaded: pd.DataFrame) -> None:
    """Reconciliación después de cargar: lo leído del archivo debe ser igual a lo escrito."""
    failures = []
    if len(loaded) != len(expected):
        failures.append(f"filas: se escribieron {len(expected)} y se leyeron {len(loaded)}")
    if list(loaded.columns) != list(expected.columns):
        failures.append(f"columnas distintas: {list(loaded.columns)}")
    if not failures:
        try:
            pd.testing.assert_frame_equal(
                loaded.reset_index(drop=True), expected.reset_index(drop=True), check_exact=False
            )
        except AssertionError as exc:
            failures.append(f"contenido distinto: {str(exc).splitlines()[0]}")
    if failures:
        raise ValidationError("load", failures)
    logger.info("Reconciliación de la carga OK: %d filas", len(loaded))
