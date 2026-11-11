"""Contrato del evento de felicidad (Parte B).

Única fuente de verdad de la forma del evento que viaja por Kafka: el producer
arma cada evento con build_event y el consumer valida cada mensaje con
validate_event antes de predecir.

Estados de validación (los del taller):
  VALID            cumple el contrato.
  INVALID_SCHEMA   no se puede interpretar: no es JSON, falta un campo o un
                   campo tiene un tipo que no corresponde.
  INVALID_VALUES   la forma es correcta pero algún valor está fuera de rango.
PREDICTION_ERROR no sale de aquí: lo asigna el consumer si el modelo falla.

Los rangos son los de la compuerta de calidad de la Parte A (config.py), para
que el streaming acepte exactamente lo que el ETL dio por limpio.
Aquí no hay Kafka ni base de datos: solo funciones puras, fáciles de probar.
"""
import json
import math
import operator
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from happiness_etl.config import COUNTRY, FEATURES, SCORE_RANGE, TARGET, YEAR, YEARS

# Campos del evento que no existen en el dataset unificado
EVENT_ID = "event_id"
ACTUAL_SCORE = "actual_happiness_score"  # el puntaje real: solo para evaluar, nunca es variable
EVENT_TIME = "event_time"

NUMERIC_FIELDS = (*FEATURES, ACTUAL_SCORE)
# Orden del formato JSON exigido por el taller
EVENT_FIELDS = (EVENT_ID, COUNTRY, YEAR, *FEATURES, ACTUAL_SCORE, EVENT_TIME)

# Estados de un evento crudo. Coinciden con el CHECK de raw_happiness_events.
RECEIVED = "RECEIVED"
VALID = "VALID"
INVALID_SCHEMA = "INVALID_SCHEMA"
INVALID_VALUES = "INVALID_VALUES"
PREDICTION_ERROR = "PREDICTION_ERROR"
STATUSES = (RECEIVED, VALID, INVALID_SCHEMA, INVALID_VALUES, PREDICTION_ERROR)


@dataclass(frozen=True)
class ValidationResult:
    """Resultado de validar un mensaje contra el contrato."""

    status: str
    errors: tuple[str, ...] = ()
    # Evento ya interpretado (solo si es válido): números como float, year como
    # int y event_time como datetime con zona horaria.
    event: dict[str, Any] | None = None

    @property
    def is_valid(self) -> bool:
        return self.status == VALID

    @property
    def error_message(self) -> str | None:
        """Todos los motivos en un solo texto (para la columna error_message)."""
        return "; ".join(self.errors) if self.errors else None


def format_event_time(moment: datetime) -> str:
    """Instante -> texto ISO-8601 en UTC con milisegundos, p. ej. 2026-11-10T19:12:00.123Z."""
    if moment.tzinfo is None:
        raise ValueError("event_time necesita zona horaria (usar datetime.now(timezone.utc))")
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_event_time(value: str) -> datetime:
    """Texto ISO-8601 -> datetime con zona horaria. Lanza ValueError si no lo es.

    El producer siempre emite en UTC. Un texto sin zona (como el ejemplo del
    taller, "2026-10-30T15:30:00") se acepta y se interpreta como UTC.
    """
    try:
        date.fromisoformat(value)
    except ValueError:
        pass
    else:
        raise ValueError("solo trae la fecha, falta la hora")
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def build_event(
    record: Mapping[str, Any], *, event_id: str | None = None, event_time: datetime | None = None
) -> dict[str, Any]:
    """Convierte una fila del dataset unificado en un evento con la forma del contrato.

    Renombra el puntaje real a actual_happiness_score, genera un event_id único
    (UUID al azar) y sella event_time con el momento de la emisión.
    """
    event: dict[str, Any] = {
        EVENT_ID: event_id if event_id is not None else str(uuid.uuid4()),
        COUNTRY: str(record[COUNTRY]),
        YEAR: operator.index(record[YEAR]),  # entero exacto: un 2019.5 falla en vez de truncarse
    }
    for name in FEATURES:
        event[name] = float(record[name])
    event[ACTUAL_SCORE] = float(record[TARGET])
    event[EVENT_TIME] = format_event_time(event_time or datetime.now(timezone.utc))
    return event


def serialize_event(event: Mapping[str, Any]) -> bytes:
    """Evento -> JSON en UTF-8 (el valor del mensaje de Kafka)."""
    return json.dumps(event, ensure_ascii=False).encode("utf-8")


def validate_event(raw: bytes | str | Mapping[str, Any] | None) -> ValidationResult:
    """Valida un mensaje (bytes o texto JSON, o un dict ya leído) contra el contrato.

    Nunca lanza excepción por el contenido del mensaje: un evento inválido es
    un resultado, no un error del programa. Reporta todos los motivos juntos.
    Los campos que sobran se ignoran.
    """
    data, error = _decode(raw)
    if error is not None:
        return ValidationResult(INVALID_SCHEMA, (error,))
    errors = _schema_errors(data)
    if errors:
        return ValidationResult(INVALID_SCHEMA, tuple(errors))
    errors = _value_errors(data)
    if errors:
        return ValidationResult(INVALID_VALUES, tuple(errors))
    return ValidationResult(VALID, (), _interpret(data))


def _decode(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Devuelve (objeto, None) o (None, motivo por el que no se pudo leer)."""
    if raw is None:
        return None, "el mensaje está vacío"
    if isinstance(raw, Mapping):
        return dict(raw), None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            return None, "el mensaje no es texto UTF-8"
    if not isinstance(raw, str):
        return None, f"el mensaje no es texto ni JSON (llegó {type(raw).__name__})"
    try:
        data = json.loads(raw)
    except (ValueError, RecursionError) as exc:
        detail = f"{exc.msg} en la posición {exc.pos}" if isinstance(exc, json.JSONDecodeError) else str(exc)
        return None, f"JSON mal formado: {detail}"
    if not isinstance(data, dict):
        return None, f"el mensaje no es un objeto JSON (llegó {_describe(data)})"
    return data, None


def _describe(value: Any) -> str:
    """Nombre legible del tipo JSON de un valor, para los mensajes de error."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "booleano"
    if isinstance(value, str):
        return f"texto {value[:30]!r}"
    if isinstance(value, int):
        return f"entero {str(value)[:30]}"
    if isinstance(value, float):
        return f"decimal {value}"
    if isinstance(value, list):
        return "lista"
    if isinstance(value, dict):
        return "objeto"
    return type(value).__name__


def _is_number(value: Any) -> bool:
    # bool es subclase de int en Python: true/false no cuentan como número
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_finite(value: int | float) -> bool:
    try:
        return math.isfinite(float(value))
    except OverflowError:  # un entero tan grande que no cabe en un float
        return False


def _schema_errors(data: dict[str, Any]) -> list[str]:
    """Campos faltantes y tipos incorrectos."""
    errors = [f"falta el campo '{name}'" for name in EVENT_FIELDS if name not in data]
    for name in (EVENT_ID, COUNTRY):
        if name in data and not (isinstance(data[name], str) and data[name].strip()):
            errors.append(f"{name} debe ser un texto no vacío (llegó {_describe(data[name])})")
    if YEAR in data and not (isinstance(data[YEAR], int) and not isinstance(data[YEAR], bool)):
        errors.append(f"{YEAR} debe ser un entero (llegó {_describe(data[YEAR])})")
    for name in NUMERIC_FIELDS:
        if name in data and not _is_number(data[name]):
            errors.append(f"{name} debe ser un número (llegó {_describe(data[name])})")
    if EVENT_TIME in data:
        value = data[EVENT_TIME]
        try:
            if not isinstance(value, str):
                raise ValueError("no es texto")
            parse_event_time(value)
        except ValueError:
            errors.append(f"{EVENT_TIME} debe ser una fecha y hora ISO-8601 (llegó {_describe(value)})")
    return errors


def _value_errors(data: dict[str, Any]) -> list[str]:
    """Valores fuera de rango. Supone que el esquema ya se validó."""
    errors = []
    if data[YEAR] not in YEARS:
        errors.append(f"{YEAR} {str(data[YEAR])[:30]} fuera de {YEARS[0]}-{YEARS[-1]}")
    for name in NUMERIC_FIELDS:
        value = data[name]
        if not _is_finite(value):
            errors.append(f"{name} no es un número finito")
        elif name == ACTUAL_SCORE:
            if not SCORE_RANGE[0] <= value <= SCORE_RANGE[1]:
                errors.append(f"{name} {value} fuera de {SCORE_RANGE[0]}-{SCORE_RANGE[1]}")
        elif value < 0:
            errors.append(f"{name} es negativo ({value})")
    return errors


def _interpret(data: dict[str, Any]) -> dict[str, Any]:
    """Evento válido con tipos de Python listos para usar (solo los campos del contrato)."""
    event: dict[str, Any] = {EVENT_ID: data[EVENT_ID], COUNTRY: data[COUNTRY], YEAR: data[YEAR]}
    for name in NUMERIC_FIELDS:
        event[name] = float(data[name])
    event[EVENT_TIME] = parse_event_time(data[EVENT_TIME])
    return event
