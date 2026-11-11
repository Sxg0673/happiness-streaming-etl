"""Pruebas del contrato del evento: qué se acepta, qué se rechaza y con qué estado."""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from happiness_etl import config, contract
from happiness_etl.contract import (
    INVALID_SCHEMA,
    INVALID_VALUES,
    VALID,
    build_event,
    serialize_event,
    validate_event,
)
from happiness_etl.load import read_processed

# Ejemplo del enunciado del taller, tal cual (event_time sin zona horaria)
PDF_EXAMPLE = {
    "event_id": "E001",
    "country": "Colombia",
    "year": 2019,
    "gdp": 1.2,
    "family": 0.8,
    "health": 0.9,
    "freedom": 0.6,
    "generosity": 0.3,
    "corruption": 0.1,
    "actual_happiness_score": 6.2,
    "event_time": "2026-10-30T15:30:00",
}
ROW = {"country": "Colombia", "year": 2019, "gdp": 1.2, "family": 0.8, "health": 0.9,
       "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "happiness_score": 6.2}


def changed(**fields):
    """El ejemplo del taller con algunos campos cambiados."""
    return {**PDF_EXAMPLE, **fields}


# --- Eventos válidos ---------------------------------------------------------

def test_pdf_example_is_valid_and_naive_time_is_utc():
    result = validate_event(PDF_EXAMPLE)
    assert result.status == VALID and result.is_valid
    assert result.errors == () and result.error_message is None
    assert result.event["event_time"] == datetime(2026, 10, 30, 15, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize("as_type", [dict, json.dumps, lambda e: json.dumps(e).encode("utf-8")])
def test_accepts_dict_text_and_bytes(as_type):
    assert validate_event(as_type(PDF_EXAMPLE)).is_valid


def test_valid_event_is_interpreted_with_python_types():
    event = validate_event(changed(gdp=1, event_time="2026-10-30T10:30:00-05:00")).event
    assert list(event) == list(contract.EVENT_FIELDS)
    assert isinstance(event["gdp"], float) and isinstance(event["year"], int)
    # La hora con zona se conserva como el mismo instante
    assert event["event_time"] == datetime(2026, 10, 30, 15, 30, tzinfo=timezone.utc)


def test_extra_fields_are_ignored():
    result = validate_event(changed(source="sensor-7"))
    assert result.is_valid and "source" not in result.event


@pytest.mark.parametrize("value", [0, 0.0, 2.5])
def test_zero_and_large_features_are_valid(value):
    assert validate_event(changed(gdp=value)).is_valid


@pytest.mark.parametrize("score", [0, 10])
def test_score_limits_are_inclusive(score):
    assert validate_event(changed(actual_happiness_score=score)).is_valid


# --- INVALID_SCHEMA ----------------------------------------------------------

@pytest.mark.parametrize(
    "raw, reason",
    [
        (None, "vacío"),
        (b"", "JSON mal formado"),
        ('{"event_id": "E001", "country": "Col', "JSON mal formado"),
        ("esto no es json", "JSON mal formado"),
        (b"\xff\xfe\x00", "UTF-8"),
        ("[1, 2, 3]", "no es un objeto JSON"),
        ('"solo texto"', "no es un objeto JSON"),
        ("null", "no es un objeto JSON"),
        (12345, "no es texto"),
    ],
)
def test_unreadable_message_is_invalid_schema(raw, reason):
    result = validate_event(raw)
    assert result.status == INVALID_SCHEMA and result.event is None
    assert len(result.errors) == 1 and reason in result.errors[0]


@pytest.mark.parametrize("field", contract.EVENT_FIELDS)
def test_each_missing_field_is_invalid_schema(field):
    event = {k: v for k, v in PDF_EXAMPLE.items() if k != field}
    result = validate_event(event)
    assert result.status == INVALID_SCHEMA
    assert result.errors == (f"falta el campo '{field}'",)


@pytest.mark.parametrize(
    "field, value",
    [
        ("event_id", ""), ("event_id", "   "), ("event_id", 7), ("event_id", None),
        ("country", ""), ("country", " \t"), ("country", None), ("country", ["Colombia"]),
        ("year", "2019"), ("year", 2019.5), ("year", 2019.0), ("year", True), ("year", None),
        ("gdp", "alto"), ("gdp", "1.2"), ("gdp", None), ("gdp", True), ("gdp", [1.2]),
        ("generosity", "0.3"),
        ("actual_happiness_score", "6.2"), ("actual_happiness_score", None),
        ("event_time", "ayer"), ("event_time", ""), ("event_time", 20261030),
        ("event_time", None), ("event_time", "2026-10-30"), ("event_time", "2026-13-45T00:00:00"),
    ],
)
def test_wrong_type_is_invalid_schema(field, value):
    result = validate_event(changed(**{field: value}))
    assert result.status == INVALID_SCHEMA
    assert len(result.errors) == 1 and result.errors[0].startswith(field)


# --- INVALID_VALUES ----------------------------------------------------------

@pytest.mark.parametrize(
    "field, value",
    [
        ("year", 2014), ("year", 2020), ("year", 0), ("year", -2019), ("year", 10**30),
        ("gdp", -0.1), ("family", -1), ("corruption", -1e-9), ("generosity", -5),
        ("gdp", float("nan")), ("health", float("inf")), ("freedom", 10**400),
        ("actual_happiness_score", -0.01), ("actual_happiness_score", 10.01),
        ("actual_happiness_score", float("nan")),
    ],
)
def test_out_of_range_is_invalid_values(field, value):
    result = validate_event(changed(**{field: value}))
    assert result.status == INVALID_VALUES and result.event is None
    assert len(result.errors) == 1 and result.errors[0].startswith(field)


def test_nan_written_in_the_json_text_is_invalid_values():
    """json.loads acepta NaN e Infinity aunque no son JSON estándar: no deben llegar al modelo."""
    text = json.dumps(PDF_EXAMPLE).replace("1.2", "NaN").replace("0.8", "-Infinity")
    result = validate_event(text)
    assert result.status == INVALID_VALUES and len(result.errors) == 2


# --- Varias fallas a la vez --------------------------------------------------

def test_all_errors_are_reported_together():
    event = {k: v for k, v in changed(year="2019").items() if k not in ("gdp", "health")}
    result = validate_event(event)
    assert result.status == INVALID_SCHEMA and len(result.errors) == 3
    assert result.error_message == "; ".join(result.errors)


def test_schema_problems_take_precedence_over_values():
    """Con el esquema roto no se evalúan rangos: el estado es INVALID_SCHEMA."""
    result = validate_event(changed(gdp="alto", year=2020))
    assert result.status == INVALID_SCHEMA and len(result.errors) == 1


def test_several_value_errors():
    result = validate_event(changed(year=2020, gdp=-1, actual_happiness_score=12))
    assert result.status == INVALID_VALUES and len(result.errors) == 3


# --- build_event y serialize_event -------------------------------------------

def test_build_event_has_the_contract_shape():
    event = build_event(ROW)
    assert list(event) == list(contract.EVENT_FIELDS)
    assert event["actual_happiness_score"] == 6.2 and config.TARGET not in event
    assert validate_event(event).is_valid


def test_build_event_ids_are_unique_and_time_is_utc_now():
    before = datetime.now(timezone.utc) - timedelta(seconds=1)
    first, second = build_event(ROW), build_event(ROW)
    assert first["event_id"] != second["event_id"]
    assert first["event_time"].endswith("Z")
    emitted = contract.parse_event_time(first["event_time"])
    assert before <= emitted <= datetime.now(timezone.utc) + timedelta(seconds=1)


def test_build_event_accepts_given_id_and_time():
    moment = datetime(2026, 11, 10, 14, 12, 0, 123000, tzinfo=timezone(timedelta(hours=-5)))
    event = build_event(ROW, event_id="E001", event_time=moment)
    assert event["event_id"] == "E001"
    assert event["event_time"] == "2026-11-10T19:12:00.123Z"  # Bogotá -> UTC


def test_build_event_converts_numpy_types_to_plain_json():
    row = {k: (np.int64(v) if k == "year" else np.float64(v) if k != "country" else v) for k, v in ROW.items()}
    event = build_event(row)
    assert type(event["year"]) is int and type(event["gdp"]) is float
    assert json.loads(serialize_event(event)) == event


def test_build_event_rejects_fractional_year_and_naive_time():
    with pytest.raises(TypeError):
        build_event({**ROW, "year": 2019.5})
    with pytest.raises(ValueError):
        build_event(ROW, event_time=datetime(2026, 11, 10, 19, 12))


def test_serialize_keeps_non_ascii_country_names():
    payload = serialize_event(build_event({**ROW, "country": "Côte d'Ivoire"}))
    assert "Côte d'Ivoire".encode("utf-8") in payload
    assert validate_event(payload).event["country"] == "Côte d'Ivoire"


# --- Coherencia con el resto del proyecto ------------------------------------

def test_every_streaming_record_produces_a_valid_event():
    """Las 235 filas que transmite el producer cumplen el contrato."""
    records = read_processed(config.STREAMING_FILE).to_dict("records")
    results = [validate_event(serialize_event(build_event(r))) for r in records]
    assert len(results) == 235 and all(r.is_valid for r in results)


def test_event_fields_match_the_unified_schema():
    """El evento lleva las mismas variables del dataset unificado, sin el puntaje como 'happiness_score'."""
    assert set(contract.EVENT_FIELDS) - {"event_id", "event_time", "actual_happiness_score"} == set(
        config.UNIFIED_COLUMNS
    ) - {config.TARGET}
    assert set(config.MODEL_FEATURES) <= set(contract.EVENT_FIELDS)
    assert contract.ACTUAL_SCORE not in config.MODEL_FEATURES


def test_statuses_match_the_database_check():
    sql = (config.ROOT_DIR / "sql" / "create_tables.sql").read_text(encoding="utf-8")
    check = sql.split("CONSTRAINT ck_raw_status CHECK", 1)[1].split(")", 1)[0]
    assert {s.strip(" '\n") for s in check.split("(", 2)[2].split(",")} == set(contract.STATUSES)


def test_score_range_matches_the_fact_table_check():
    """El rango del contrato es el mismo que exige fact_predictions.actual_score."""
    sql = (config.ROOT_DIR / "sql" / "create_tables.sql").read_text(encoding="utf-8")
    low, high = config.SCORE_RANGE
    assert f"CHECK (actual_score BETWEEN {low:g} AND {high:g})" in sql
