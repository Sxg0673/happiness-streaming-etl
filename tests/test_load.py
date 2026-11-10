"""Pruebas del módulo load: ida y vuelta del CSV y escritura atómica."""
import pandas as pd
import pytest

from happiness_etl import load
from happiness_etl.clean import clean_all
from happiness_etl.extract import read_all
from happiness_etl.transform import transform


@pytest.fixture(scope="module")
def unified():
    return transform(clean_all(read_all()))


def test_round_trip_keeps_values_and_types(unified, tmp_path):
    path = load.write_processed(unified, tmp_path / "out.csv")
    pd.testing.assert_frame_equal(load.read_processed(path), unified, check_exact=True)


def test_csv_has_no_index_column(unified, tmp_path):
    path = load.write_processed(unified, tmp_path / "out.csv")
    header = path.read_text(encoding="utf-8").splitlines()[0]
    assert header == ",".join(unified.columns)


def test_no_temp_files_left(unified, tmp_path):
    load.write_processed(unified, tmp_path / "out.csv")
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]


def test_failed_write_keeps_previous_file(unified, tmp_path, monkeypatch):
    """Si la escritura falla a mitad, el archivo anterior queda intacto y no queda basura."""
    path = tmp_path / "out.csv"
    path.write_text("anterior", encoding="utf-8")

    def broken_to_csv(self, *args, **kwargs):
        raise OSError("disco lleno")

    monkeypatch.setattr(pd.DataFrame, "to_csv", broken_to_csv)
    with pytest.raises(OSError):
        load.write_processed(unified, path)
    assert path.read_text(encoding="utf-8") == "anterior"
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]


def test_written_file_is_readable_by_others(unified, tmp_path):
    path = load.write_processed(unified, tmp_path / "out.csv")
    assert path.stat().st_mode & 0o044  # lectura para grupo y otros (según umask)
