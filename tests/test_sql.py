"""Coherencia de los scripts SQL con el pipeline (no necesita una base de datos)."""
import re

import pytest

from happiness_etl.clean import clean_all
from happiness_etl.config import COUNTRY, ROOT_DIR, YEARS
from happiness_etl.extract import read_all
from happiness_etl.transform import transform

SQL_DIR = ROOT_DIR / "sql"
CREATE = (SQL_DIR / "create_tables.sql").read_text(encoding="utf-8")
SEED = (SQL_DIR / "seed_dimensions.sql").read_text(encoding="utf-8")


def _seed_countries():
    """Pares (país, región) de los VALUES del INSERT de dim_country."""
    bloque = SEED.split("INSERT INTO dim_country", 1)[1]
    pares = re.findall(r"\(\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*\)", bloque)
    return {p.replace("''", "'"): r.replace("''", "'") for p, r in pares}


@pytest.fixture(scope="module")
def unified():
    return transform(clean_all(read_all()))


def test_seed_covers_every_country_of_the_pipeline(unified):
    """Cada país que puede llegar en un evento tiene fila (y región) en dim_country."""
    seed = _seed_countries()
    assert set(unified[COUNTRY]) == set(seed)
    assert all(seed.values())


def test_seed_covers_every_year():
    """dim_date tiene un registro por año del dataset y ninguno más."""
    bloque = SEED.split("INSERT INTO dim_date", 1)[1].split("INSERT INTO", 1)[0]
    anios = {int(a) for a in re.findall(r"\(\s*(\d{4})\s*,\s*\1\s*,", bloque)}
    assert anios == set(YEARS)


@pytest.mark.parametrize("tabla", ["raw_happiness_events", "fact_predictions", "dim_country", "dim_date"])
def test_required_tables_exist(tabla):
    """Tablas mínimas que pide el PDF (Paso 7)."""
    assert f"CREATE TABLE IF NOT EXISTS {tabla} (" in CREATE


@pytest.mark.parametrize("estado", ["VALID", "INVALID_SCHEMA", "INVALID_VALUES", "PREDICTION_ERROR"])
def test_raw_status_includes_pdf_statuses(estado):
    """Los estados de procesamiento del PDF están permitidos en raw_happiness_events."""
    check = re.search(r"CONSTRAINT ck_raw_status CHECK \((.*?)\)\s*,", CREATE, re.S).group(1)
    assert f"'{estado}'" in check


@pytest.mark.parametrize("columna", [
    "prediction_id", "raw_event_id", "country_id", "date_id", "actual_score",
    "predicted_score", "prediction_error", "event_time", "prediction_timestamp",
])
def test_fact_has_suggested_columns(columna):
    """Columnas sugeridas por el PDF para fact_predictions (Pasos 7 y 8)."""
    fact = CREATE.split("CREATE TABLE IF NOT EXISTS fact_predictions (", 1)[1].split(");", 1)[0]
    assert re.search(rf"^\s+{columna}\s", fact, re.M), columna
