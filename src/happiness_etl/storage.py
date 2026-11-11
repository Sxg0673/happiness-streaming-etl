"""Storage: lo que el consumer escribe en PostgreSQL.

Cada función es una transacción completa (la conexión debe estar en autocommit):
  1. insert_raw        guarda el mensaje tal como llegó, en estado RECEIVED.
  2. mark_invalid      cierra un evento que no se va a predecir, con su motivo.
  3. save_prediction   guarda la predicción y marca el crudo como VALID, todo o nada.

La terna (topic, partición, offset) identifica un mensaje de Kafka: si Kafka lo
reentrega, insert_raw devuelve la fila que ya existía en vez de duplicarla.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import psycopg

from happiness_etl.config import YEAR
from happiness_etl.contract import ACTUAL_SCORE, EVENT_ID, EVENT_TIME, RECEIVED, VALID

REPLACEMENT = "�"  # el signo � con el que se marca lo que no se pudo leer


@dataclass(frozen=True)
class RawEvent:
    """Fila de raw_happiness_events recién insertada o ya existente."""

    raw_event_id: int
    status: str
    is_new: bool  # False = Kafka reentregó un mensaje que ya estaba guardado

    @property
    def is_pending(self) -> bool:
        """Guardado pero sin procesar: hay que validarlo y predecirlo."""
        return self.status == RECEIVED


def to_text(payload: bytes | None) -> tuple[str, str | None]:
    """Mensaje de Kafka -> (texto para raw_payload, nota si no se pudo guardar exacto).

    Un mensaje de texto UTF-8 se guarda idéntico. La columna es de texto, así que
    tres casos no caben tal cual y se guardan en versión legible, con una nota.
    """
    if payload is None:
        return "", "el mensaje llegó sin contenido"
    try:
        text, note = payload.decode("utf-8"), None
    except UnicodeDecodeError:
        text = payload.decode("utf-8", errors="replace")
        note = f"contenido que no es UTF-8: los bytes ilegibles se guardaron como {REPLACEMENT}"
    if "\x00" in text:  # PostgreSQL no admite el carácter nulo en un texto
        text = text.replace("\x00", REPLACEMENT)
        note = note or f"contenía caracteres nulos: se guardaron como {REPLACEMENT}"
    return text, note


def insert_raw(
    connection: psycopg.Connection,
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    timestamp: datetime | None,
    payload: bytes | None,
) -> tuple[RawEvent, str | None]:
    """Paso 1: guarda el mensaje crudo antes de validar o predecir.

    Devuelve la fila y la nota de to_text. Si el mensaje ya estaba guardado
    (reentrega), devuelve la fila existente con su estado actual.
    """
    text, note = to_text(payload)
    key_text = None if key is None else to_text(key)[0]
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_happiness_events
                (kafka_topic, kafka_partition, kafka_offset, kafka_key, kafka_timestamp, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
            RETURNING raw_event_id, status
            """,
            (topic, partition, offset, key_text, timestamp, text),
        )
        row = cursor.fetchone()
        if row is not None:
            return RawEvent(row[0], row[1], is_new=True), note
        cursor.execute(
            """
            SELECT raw_event_id, status FROM raw_happiness_events
            WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s
            """,
            (topic, partition, offset),
        )
        row = cursor.fetchone()
        return RawEvent(row[0], row[1], is_new=False), note


def mark_invalid(connection: psycopg.Connection, raw_event_id: int, status: str, error_message: str) -> None:
    """Cierra un evento sin predicción: estado (INVALID_* o PREDICTION_ERROR) y motivo."""
    with connection.transaction():
        connection.execute(
            """
            UPDATE raw_happiness_events
            SET status = %s, error_message = %s, processed_at = now()
            WHERE raw_event_id = %s
            """,
            (status, error_message, raw_event_id),
        )


def _country_id(cursor: psycopg.Cursor, name: str) -> int:
    """Id del país; si no está en la dimensión, lo agrega con región vacía."""
    cursor.execute(
        "INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING", (name,)
    )
    cursor.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (name,))
    return cursor.fetchone()[0]


def save_prediction(
    connection: psycopg.Connection,
    raw_event_id: int,
    event: Mapping[str, Any],
    country_name: str,
    predicted_score: float,
    model_version: str,
    note: str | None = None,
) -> None:
    """Guarda la predicción y marca el crudo como VALID en una sola transacción."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO fact_predictions
                (raw_event_id, event_id, country_id, date_id, actual_score, predicted_score, event_time, model_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (raw_event_id) DO NOTHING
            """,
            (
                raw_event_id,
                event[EVENT_ID],
                _country_id(cursor, country_name),
                event[YEAR],  # date_id es el año
                event[ACTUAL_SCORE],
                predicted_score,
                event[EVENT_TIME],
                model_version,
            ),
        )
        cursor.execute(
            """
            UPDATE raw_happiness_events
            SET status = %s, error_message = %s, processed_at = now()
            WHERE raw_event_id = %s
            """,
            (VALID, note, raw_event_id),
        )
