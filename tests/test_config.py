"""Pruebas de consistencia de la configuración."""
from happiness_etl import config


def test_every_year_maps_all_canonical_columns():
    """Cada año debe poder producir todas las columnas canónicas salvo 'year'."""
    expected = set(config.UNIFIED_COLUMNS) - {config.YEAR}
    for year in config.YEARS:
        assert set(config.COLUMN_MAP_BY_YEAR[year].values()) == expected, year


def test_mapped_names_are_unique_per_year():
    """Dos columnas originales no pueden apuntar al mismo nombre canónico."""
    for year, mapping in config.COLUMN_MAP_BY_YEAR.items():
        assert len(set(mapping.values())) == len(mapping), year


def test_model_features_are_a_subset_of_features():
    """El modelo solo usa variables del esquema; no incluye 'year', 'country' ni el objetivo."""
    assert set(config.MODEL_FEATURES) <= set(config.FEATURES)
    assert "generosity" not in config.MODEL_FEATURES
    assert not {config.YEAR, config.COUNTRY, config.TARGET} & set(config.MODEL_FEATURES)


def test_target_and_country_are_not_features():
    """El objetivo y el identificador no pueden ser variables del modelo."""
    assert config.TARGET not in config.FEATURES
    assert config.COUNTRY not in config.FEATURES
