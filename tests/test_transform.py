"""Pruebas del módulo transform: con los datos reales y con casos pequeños inventados."""
import numpy as np
import pandas as pd
import pytest

from happiness_etl import config
from happiness_etl.clean import clean_all
from happiness_etl.extract import read_all
from happiness_etl.transform import interpolate_missing, standardize_country_names, transform


@pytest.fixture(scope="module")
def unified():
    return transform(clean_all(read_all()))


def _frame(country, years, corruption):
    """DataFrame mínimo con el esquema unificado (variables en 0.5 salvo corruption)."""
    n = len(years)
    data = {config.COUNTRY: [country] * n, config.YEAR: years}
    data.update({col: [0.5] * n for col in config.FEATURES})
    data["corruption"] = corruption
    data[config.TARGET] = [5.0] * n
    return pd.DataFrame(data)[list(config.UNIFIED_COLUMNS)]


def test_shape_and_columns(unified):
    assert len(unified) == 782
    assert list(unified.columns) == list(config.UNIFIED_COLUMNS)


def test_no_nulls_left(unified):
    assert not unified.isna().any().any()


def test_country_names_unified(unified):
    assert unified[config.COUNTRY].nunique() == 164
    assert not unified[config.COUNTRY].isin(config.COUNTRY_NAME_MAP.keys()).any()
    assert not unified.duplicated([config.COUNTRY, config.YEAR]).any()


def test_uae_2018_is_interpolated(unified):
    row = unified[(unified[config.COUNTRY] == "United Arab Emirates") & (unified[config.YEAR] == 2018)]
    assert row["corruption"].iloc[0] == pytest.approx((0.324489563703537 + 0.182) / 2)


def test_sorted_by_year_and_country(unified):
    expected = unified.sort_values([config.YEAR, config.COUNTRY]).reset_index(drop=True)
    pd.testing.assert_frame_equal(unified, expected)


def test_standardize_country_names():
    df = _frame("Trinidad & Tobago", [2018], [0.1])
    assert standardize_country_names(df)[config.COUNTRY].iloc[0] == "Trinidad and Tobago"


def test_interpolation_never_extrapolates():
    """Un nulo en el primer o último año del país se deja: validate debe frenarlo."""
    df = _frame("X", [2015, 2016, 2017], [np.nan, 0.2, np.nan])
    out = interpolate_missing(df)
    assert out["corruption"].isna().sum() == 2


def test_interpolation_weights_by_year_gap():
    """Si al país le falta un año, la interpolación respeta la distancia real."""
    df = _frame("X", [2015, 2016, 2019], [0.1, np.nan, 0.5])
    out = interpolate_missing(df)
    assert out.loc[out[config.YEAR] == 2016, "corruption"].iloc[0] == pytest.approx(0.2)


def test_interpolation_stays_within_country():
    df = pd.concat([_frame("A", [2015, 2016], [0.1, np.nan]), _frame("B", [2017], [0.9])], ignore_index=True)
    out = interpolate_missing(df)
    assert out.loc[out[config.COUNTRY] == "A", "corruption"].isna().sum() == 1
