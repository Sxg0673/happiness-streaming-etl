"""Load: escritura y lectura del dataset unificado en data/processed.

La escritura es atómica: se escribe en un archivo temporal de la misma carpeta
y luego se renombra. Si algo falla a mitad, el archivo final queda como estaba
(o no existe): nunca hay un CSV a medias.
Como el CSV no guarda tipos, read_processed los fija al leer.
"""
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

from happiness_etl.config import COUNTRY, FEATURES, PROCESSED_FILE, TARGET, UNIFIED_COLUMNS, YEAR

logger = logging.getLogger(__name__)

DTYPES = {COUNTRY: "str", YEAR: "int64", **{col: "float64" for col in (*FEATURES, TARGET)}}


def _current_umask() -> int:
    mask = os.umask(0)
    os.umask(mask)
    return mask


def write_processed(df: pd.DataFrame, path: Path = PROCESSED_FILE) -> Path:
    """Escribe el dataset en CSV (sin índice) de forma atómica y devuelve la ruta."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            df[list(UNIFIED_COLUMNS)].to_csv(handle, index=False)
        # mkstemp crea el archivo solo para el dueño (0600): se dejan los permisos normales
        os.chmod(tmp_name, 0o666 & ~_current_umask())
        os.replace(tmp_name, path)  # renombrado atómico dentro de la misma carpeta
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    logger.info("Dataset escrito: %s (%d filas)", path, len(df))
    return path


def read_processed(path: Path = PROCESSED_FILE) -> pd.DataFrame:
    """Lee el dataset unificado con los tipos del esquema."""
    return pd.read_csv(path, dtype=DTYPES)[list(UNIFIED_COLUMNS)]
