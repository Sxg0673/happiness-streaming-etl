"""Pruebas del módulo validate: el dataset real pasa y cada regla rota se detecta."""
import numpy as np
import pytest

from happiness_etl import config
from happiness_etl.clean import clean_all
from happiness_etl.extract import read_all
from happiness_etl.transform import transform
from happiness_etl.validate import ValidationError, check_dataset, validate_dataset, validate_load


@pytest.fixture(scope="module")
def unified():
    return transform(clean_all(read_all()))


@pytest.fixture
def df(unified):
    """Copia para poder romperla en cada prueba."""
    return unified.copy()


def test_real_dataset_passes(unified):
    assert validate_dataset(unified) is unified


def test_wrong_columns(df):
    failures = check_dataset(df.drop(columns=["generosity"]))
    assert len(failures) == 1 and "columnas" in failures[0]


@pytest.mark.parametrize(
    "column, value, rule",
    [
        ("corruption", np.nan, "nulas"),
        (config.COUNTRY, "  ", "country vacío"),
        (config.YEAR, 2020, "year fuera"),
        ("gdp", -0.1, "negativas"),
        (config.TARGET, 11.0, config.TARGET),
    ],
)
def test_each_rule_detects_its_problem(df, column, value, rule):
    df.loc[0, column] = value
    failures = check_dataset(df)
    assert any(rule in f for f in failures), failures


def test_duplicate_country_year(df):
    df.loc[1, [config.COUNTRY, config.YEAR]] = df.loc[0, [config.COUNTRY, config.YEAR]].to_numpy()
    assert any("repetido" in f for f in check_dataset(df))


def test_collects_all_failures(df):
    """La compuerta reporta todas las fallas juntas, no solo la primera."""
    df.loc[0, "gdp"] = -1.0
    df.loc[1, config.TARGET] = 12.0
    with pytest.raises(ValidationError) as info:
        validate_dataset(df)
    assert len(info.value.failures) == 2
    assert info.value.stage == "dataset"


def test_load_reconciliation_ok(unified):
    validate_load(unified, unified.copy())


def test_load_reconciliation_detects_missing_rows(unified):
    with pytest.raises(ValidationError, match="filas"):
        validate_load(unified, unified.iloc[:-1])


def test_load_reconciliation_detects_changed_values(unified):
    loaded = unified.copy()
    loaded.loc[0, "gdp"] += 1
    with pytest.raises(ValidationError, match="contenido"):
        validate_load(unified, loaded)
