"""Clean: limpieza de cada archivo por separado (una fuente a la vez).

Para cada año: nombres canónicos de columna, columna year, texto del país
normalizado, tipos numéricos y duplicados exactos dentro del año.
No une años ni completa nulos: eso necesita ver varias fuentes y va en
transform.py (unificación de nombres de país e interpolación del nulo).
"""
import logging
import re

import pandas as pd

from happiness_etl.config import (
    COLUMN_MAP_BY_YEAR,
    COUNTRY,
    COUNTRY_NAME_MAP,
    FEATURES,
    TARGET,
    UNIFIED_COLUMNS,
    YEAR,
)

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS = (*FEATURES, TARGET)


def _normalize_text(value: str) -> str:
    """Quita espacios al inicio y al final y deja un solo espacio entre palabras."""
    return re.sub(r"\s+", " ", value).strip()


def canonical_country(name: str) -> str:
    """Nombre de país tal como quedó en el dataset unificado.

    Mismo tratamiento que recibieron los datos de entrenamiento (espacios y
    variantes de nombre). Lo usa el consumer para que "Trinidad & Tobago" o
    " Colombia " no creen un país distinto en dim_country.
    """
    text = _normalize_text(name)
    return COUNTRY_NAME_MAP.get(text, text)


def clean_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Limpia el DataFrame crudo de un año y lo deja con el esquema unificado."""
    if year not in COLUMN_MAP_BY_YEAR:
        raise ValueError(f"Año no soportado: {year}")
    mapping = COLUMN_MAP_BY_YEAR[year]

    # Esquema: deben estar todas las columnas esperadas; las demás se descartan
    missing = [col for col in mapping if col not in df.columns]
    if missing:
        raise ValueError(f"{year}: faltan columnas en el archivo: {missing}")
    out = df[list(mapping)].rename(columns=mapping)
    out[YEAR] = year

    # Formato del país
    out[COUNTRY] = out[COUNTRY].astype(str).map(_normalize_text)

    # Tipos: un valor no numérico es un error de origen y detiene el proceso
    for col in NUMERIC_COLUMNS:
        try:
            out[col] = pd.to_numeric(out[col]).astype("float64")
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{year}: la columna '{col}' tiene valores no numéricos") from exc
    out[YEAR] = out[YEAR].astype("int64")

    # Duplicados exactos dentro del año (los de país con valores distintos los detecta validate)
    duplicated = int(out.duplicated().sum())
    if duplicated:
        logger.warning("%s: se eliminan %d filas duplicadas", year, duplicated)
        out = out.drop_duplicates()

    nulls = int(out[list(NUMERIC_COLUMNS)].isna().sum().sum())
    logger.info("%s: %d filas limpias, %d nulos (se tratan en transform)", year, len(out), nulls)
    return out[list(UNIFIED_COLUMNS)].reset_index(drop=True)


def clean_all(frames: dict[int, pd.DataFrame]) -> dict[int, pd.DataFrame]:
    """Aplica clean_year a cada año: {año: DataFrame limpio}."""
    return {year: clean_year(df, year) for year, df in frames.items()}
