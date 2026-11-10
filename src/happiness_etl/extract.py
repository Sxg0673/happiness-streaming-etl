"""Extract: lectura de los CSV originales, sin ninguna transformación.

Devuelve los datos tal cual vienen (mismos nombres de columna y valores).
La limpieza va en clean.py y la unificación en transform.py.
"""
import pandas as pd

from happiness_etl.config import RAW_DIR, YEARS


def read_raw(year: int) -> pd.DataFrame:
    """Lee el CSV original de un año."""
    if year not in YEARS:
        raise ValueError(f"Año no soportado: {year}. Años disponibles: {YEARS}")
    return pd.read_csv(RAW_DIR / f"{year}.csv")


def read_all() -> dict[int, pd.DataFrame]:
    """Lee los CSV de todos los años: {año: DataFrame}."""
    return {year: read_raw(year) for year in YEARS}
