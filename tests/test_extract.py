"""Pruebas del módulo extract y de su coherencia con la configuración."""
import pytest

from happiness_etl import config
from happiness_etl.extract import read_all, read_raw


@pytest.mark.parametrize("year", config.YEARS)
def test_column_map_keys_exist_in_raw_file(year):
    """El mapa de equivalencias debe coincidir con las columnas reales del CSV."""
    columns = set(read_raw(year).columns)
    assert set(config.COLUMN_MAP_BY_YEAR[year]) <= columns


def test_read_all_returns_every_year():
    assert set(read_all()) == set(config.YEARS)


def test_read_raw_rejects_unknown_year():
    with pytest.raises(ValueError):
        read_raw(2014)
