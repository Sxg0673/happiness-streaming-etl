"""Pruebas del módulo clean: con los datos reales y con casos pequeños inventados."""
import pandas as pd
import pytest

from happiness_etl import config
from happiness_etl.clean import clean_all, clean_year
from happiness_etl.extract import read_all

EXPECTED_ROWS = {2015: 158, 2016: 157, 2017: 155, 2018: 156, 2019: 156}


@pytest.fixture(scope="module")
def cleaned():
    return clean_all(read_all())


def _raw_2019(**overrides) -> pd.DataFrame:
    """Archivo crudo mínimo con el esquema de 2019 (dos países)."""
    data = {
        "Overall rank": [1, 2],
        "Country or region": ["Finland", "Denmark"],
        "Score": [7.769, 7.6],
        "GDP per capita": [1.34, 1.383],
        "Social support": [1.587, 1.573],
        "Healthy life expectancy": [0.986, 0.996],
        "Freedom to make life choices": [0.596, 0.592],
        "Generosity": [0.153, 0.252],
        "Perceptions of corruption": [0.393, 0.41],
    }
    data.update(overrides)
    return pd.DataFrame(data)


@pytest.mark.parametrize("year", config.YEARS)
def test_unified_columns_and_rows(cleaned, year):
    df = cleaned[year]
    assert list(df.columns) == list(config.UNIFIED_COLUMNS)
    assert len(df) == EXPECTED_ROWS[year]
    assert (df[config.YEAR] == year).all()


def test_types(cleaned):
    df = cleaned[2015]
    assert df[config.YEAR].dtype == "int64"
    for col in (*config.FEATURES, config.TARGET):
        assert df[col].dtype == "float64"


def test_nulls_are_kept_for_transform(cleaned):
    """clean no imputa: el nulo de UAE 2018 debe seguir ahí."""
    df = cleaned[2018]
    uae = df[df[config.COUNTRY] == "United Arab Emirates"]
    assert uae["corruption"].isna().all()


def test_country_whitespace_is_normalized():
    df = clean_year(_raw_2019(**{"Country or region": ["  Finland ", "Costa   Rica"]}), 2019)
    assert df[config.COUNTRY].tolist() == ["Finland", "Costa Rica"]


def test_exact_duplicates_are_dropped():
    raw = _raw_2019()
    df = clean_year(pd.concat([raw, raw.iloc[[0]]], ignore_index=True), 2019)
    assert len(df) == 2


def test_missing_column_raises():
    with pytest.raises(ValueError, match="faltan columnas"):
        clean_year(_raw_2019().drop(columns=["Generosity"]), 2019)


def test_non_numeric_value_raises():
    with pytest.raises(ValueError, match="no numéricos"):
        clean_year(_raw_2019(Generosity=["0.1", "n/a"]), 2019)


def test_unknown_year_raises():
    with pytest.raises(ValueError):
        clean_year(_raw_2019(), 2014)
