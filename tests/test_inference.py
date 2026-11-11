"""Pruebas de inference y de las piezas puras del consumer (no necesitan Docker)."""
import json
import re

import numpy as np
import pytest

from happiness_etl import config, storage
from happiness_etl.clean import canonical_country
from happiness_etl.contract import build_event, serialize_event, validate_event
from happiness_etl.inference import Predictor
from happiness_etl.load import read_processed


@pytest.fixture(scope="module")
def predictor():
    return Predictor()


@pytest.fixture(scope="module")
def events():
    rows = read_processed(config.STREAMING_FILE).to_dict("records")
    return [validate_event(serialize_event(build_event(row))).event for row in rows]


def test_streamed_events_reproduce_the_training_metrics(predictor, events):
    """Evento -> contrato -> modelo da las mismas métricas que el notebook de entrenamiento."""
    errors = np.array([predictor.predict(e) - e["actual_happiness_score"] for e in events])
    expected = json.loads(config.METRICS_FILE.read_text(encoding="utf-8"))["test_metrics"]
    assert round(float(np.abs(errors).mean()), 4) == expected["mae"]
    assert round(float(np.sqrt((errors**2).mean())), 4) == expected["rmse"]
    assert round(float(errors.mean()), 4) == expected["bias"]


def test_model_uses_training_columns_and_never_the_actual_score(predictor, events):
    assert predictor.columns == [*config.MODEL_FEATURES, config.YEAR]
    event = events[0]
    changed = {**event, "actual_happiness_score": 0.0, "generosity": 2.9, "country": "Otro"}
    assert predictor.predict(changed) == predictor.predict(event)


def test_version_identifies_the_model_file(predictor, tmp_path):
    assert re.fullmatch(r"LinearRegression-[0-9a-f]{12}", predictor.version)
    assert Predictor().version == predictor.version


def test_model_failures_raise_instead_of_returning_garbage(predictor, events):
    with pytest.raises(ValueError):
        predictor.predict({**events[0], "year": 2020})  # segunda barrera: año que el modelo no conoce
    with pytest.raises(KeyError):
        predictor.predict({k: v for k, v in events[0].items() if k != "gdp"})


def test_missing_model_file(tmp_path):
    with pytest.raises(OSError):
        Predictor(tmp_path / "no.pkl")


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Colombia", "Colombia"), ("  Colombia ", "Colombia"), ("Trinidad & Tobago", "Trinidad and Tobago"),
        ("Hong  Kong S.A.R., China", "Hong Kong"), ("North Macedonia", "Macedonia"), ("Narnia", "Narnia"),
    ],
)
def test_canonical_country(name, expected):
    assert canonical_country(name) == expected


def test_canonical_country_leaves_every_streamed_country_unchanged(events):
    assert all(canonical_country(e["country"]) == e["country"] for e in events)


def test_to_text_keeps_utf8_messages_exact():
    payload = serialize_event(build_event({"country": "Côte d'Ivoire", "year": 2019, "gdp": 1.0, "family": 1.0,
                                           "health": 1.0, "freedom": 0.5, "generosity": 0.1, "corruption": 0.1,
                                           "happiness_score": 5.0}))
    text, note = storage.to_text(payload)
    assert text.encode("utf-8") == payload and note is None


@pytest.mark.parametrize(
    "payload, text, reason",
    [
        (None, "", "sin contenido"),
        (b"\xff\xfeabc", "��abc", "no es UTF-8"),
        (b'{"a": "x\x00y"}', '{"a": "x�y"}', "nulos"),
        (b"\xff\x00", "��", "no es UTF-8"),
    ],
)
def test_to_text_marks_what_cannot_be_stored_exactly(payload, text, reason):
    stored, note = storage.to_text(payload)
    assert stored == text and reason in note and "\x00" not in stored
