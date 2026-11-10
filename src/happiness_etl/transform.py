"""Transform: integración de los años ya limpios en un solo dataset.

Une los cinco años, unifica los nombres de país entre años y completa los
nulos con interpolación lineal dentro de cada país. Todo lo que necesita ver
más de una fuente a la vez vive aquí; la limpieza por año está en clean.py.
"""
import logging

import pandas as pd

from happiness_etl.config import COUNTRY, COUNTRY_NAME_MAP, FEATURES, UNIFIED_COLUMNS, YEAR

logger = logging.getLogger(__name__)


def concat_years(frames: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Une los DataFrames limpios de todos los años en uno solo."""
    return pd.concat(frames.values(), ignore_index=True)[list(UNIFIED_COLUMNS)]


def standardize_country_names(df: pd.DataFrame) -> pd.DataFrame:
    """Reemplaza las variantes de nombre por el nombre canónico de config."""
    changed = df[COUNTRY].isin(COUNTRY_NAME_MAP.keys())
    logger.info("Nombres de país unificados: %d filas", int(changed.sum()))
    return df.assign(**{COUNTRY: df[COUNTRY].replace(COUNTRY_NAME_MAP)})


def _interpolate_country(values: pd.Series, years: pd.Series) -> pd.Series:
    """Interpola una serie de un país usando el año como eje.

    limit_area="inside": solo rellena huecos entre dos años observados; nunca
    extrapola al primer o último año (ese nulo seguiría y validate lo frena).
    method="index" respeta los años faltantes del país (pondera por distancia).
    """
    series = pd.Series(values.to_numpy(), index=years.to_numpy())
    filled = series.interpolate(method="index", limit_area="inside")
    return pd.Series(filled.to_numpy(), index=values.index)


def interpolate_missing(df: pd.DataFrame, columns=FEATURES) -> pd.DataFrame:
    """Completa nulos de las variables con interpolación lineal por país."""
    out = df.sort_values([COUNTRY, YEAR]).copy()
    for col in columns:
        missing = out[col].isna()
        if not missing.any():
            continue
        out[col] = out.groupby(COUNTRY, group_keys=False)[col].apply(
            lambda s: _interpolate_country(s, out.loc[s.index, YEAR])
        )
        for idx in out.index[missing & out[col].notna()]:
            logger.warning(
                "Imputado por interpolación: %s %s %s = %.4f",
                out.at[idx, COUNTRY], out.at[idx, YEAR], col, out.at[idx, col],
            )
    return out


def transform(frames: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Integra los años limpios: unir, unificar países, interpolar y ordenar."""
    df = concat_years(frames)
    df = standardize_country_names(df)
    df = interpolate_missing(df)
    return df.sort_values([YEAR, COUNTRY]).reset_index(drop=True)
