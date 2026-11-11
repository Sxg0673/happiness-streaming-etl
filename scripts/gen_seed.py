"""Genera sql/seed_dimensions.sql a partir de los CSV originales de data/raw.

dim_country: un país por fila con su región. La región viene de la columna Region
de 2015 y 2016 (ningún país cambia de región entre ambos años). Gambia solo
aparece desde 2019, sin Region en la fuente, y se asigna a mano.
dim_date: un registro por reporte anual (config.YEARS).

Uso (desde la raíz del repo, con el entorno activado):
    python scripts/gen_seed.py

El archivo resultante es idempotente (ON CONFLICT DO NOTHING) y
tests/test_sql.py comprueba que cubre todos los países y años del dataset.
"""
import pandas as pd

from happiness_etl.clean import clean_all
from happiness_etl.config import COUNTRY, COUNTRY_NAME_MAP, RAW_DIR, ROOT_DIR, YEARS
from happiness_etl.extract import read_all
from happiness_etl.transform import transform

SEED_FILE = ROOT_DIR / "sql" / "seed_dimensions.sql"

# Países sin región en la fuente: se asignan a mano
MANUAL_REGIONS = {"Gambia": "Sub-Saharan Africa"}


def countries():
    """Países del dataset unificado, con los nombres canónicos de la Parte A."""
    return sorted(transform(clean_all(read_all()))[COUNTRY].unique())


def regions():
    """Región de cada país según los reportes 2015 y 2016."""
    region = {}
    for year in (2015, 2016):
        data = pd.read_csv(RAW_DIR / f"{year}.csv")
        for country, reg in zip(data["Country"].str.strip(), data["Region"].str.strip()):
            region.setdefault(COUNTRY_NAME_MAP.get(country, country), reg)
    region.update(MANUAL_REGIONS)
    return region


def quote(text):
    """Literal de texto SQL con las comillas simples escapadas."""
    return "'" + text.replace("'", "''") + "'"


def build_sql(names, region):
    """Arma el script SQL completo."""
    missing = [name for name in names if name not in region]
    if missing:
        raise ValueError(f"Países sin región: {missing}")
    rows = ",\n".join(f"    ({quote(name)}, {quote(region[name])})" for name in names)
    years = ",\n".join(f"    ({y}, {y}, DATE '{y}-01-01', 'World Happiness Report {y}')" for y in YEARS)
    return f"""-- =============================================================================
-- Carga inicial de las dimensiones (datos de referencia, no de streaming).
--
-- dim_country: los {len(names)} países del dataset unificado (nombres canónicos de
--   config.COUNTRY_NAME_MAP). La región viene de la columna Region de 2015 y 2016
--   (ningún país cambia de región entre ambos años). Gambia solo aparece desde
--   2019, sin Region en la fuente: se asigna a mano 'Sub-Saharan Africa'.
-- dim_date: un registro por reporte anual (2015-2019).
--
-- Generado desde data/raw; idempotente (ON CONFLICT DO NOTHING).
-- tests/test_sql.py comprueba que cubre todos los países y años del dataset.
-- =============================================================================

INSERT INTO dim_date (date_id, year, report_date, report_name) VALUES
{years}
ON CONFLICT (date_id) DO NOTHING;

INSERT INTO dim_country (country_name, region) VALUES
{rows}
ON CONFLICT (country_name) DO NOTHING;
"""


def main():
    names = countries()
    SEED_FILE.write_text(build_sql(names, regions()), encoding="utf-8")
    print(f"{SEED_FILE.relative_to(ROOT_DIR)}: {len(names)} países, {len(YEARS)} años")


if __name__ == "__main__":
    main()
