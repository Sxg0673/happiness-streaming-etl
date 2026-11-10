"""Pruebas de punta a punta del orquestador."""
import pytest

from happiness_etl import config, main
from happiness_etl.load import read_processed
from happiness_etl.validate import ValidationError


def test_run_writes_valid_dataset(tmp_path):
    path = main.run(tmp_path / "out.csv")
    df = read_processed(path)
    assert len(df) == 782
    assert not df.isna().any().any()


def test_gate_stops_before_writing(tmp_path, monkeypatch):
    """Si la compuerta falla, no se escribe ningún archivo."""
    real_transform = main.transform

    def broken_transform(frames):
        df = real_transform(frames)
        df.loc[0, "gdp"] = -1.0
        return df

    monkeypatch.setattr(main, "transform", broken_transform)
    with pytest.raises(ValidationError):
        main.run(tmp_path / "out.csv")
    assert not any(tmp_path.iterdir())


def test_main_returns_1_on_validation_error(monkeypatch):
    def failing_run():
        raise ValidationError("dataset", ["variables negativas: 1"])

    monkeypatch.setattr(main, "run", failing_run)
    assert main.main() == 1


def test_main_returns_0_on_success(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "run", lambda: tmp_path / "out.csv")
    assert main.main() == 0
    assert config.PROCESSED_FILE.name == "happiness_unified.csv"
