"""Consumer de Kafka: guarda cada evento crudo, lo valida, predice y guarda la predicción.

Por cada mensaje del topic happiness-events (grupo happiness-ml-team):
  1. Guarda el mensaje tal como llegó en raw_happiness_events (estado RECEIVED).
  2. Lo valida contra el contrato. Si es inválido lo marca INVALID_SCHEMA o
     INVALID_VALUES con el motivo y sigue con el siguiente.
  3. Predice con models/model.pkl. Si el modelo falla, lo marca PREDICTION_ERROR.
  4. Guarda la predicción en fact_predictions y marca el crudo como VALID.
  5. Confirma el offset en Kafka, solo después de que la base guardó todo.

Un evento malo nunca detiene el consumer. Una falla de la infraestructura
(Kafka o PostgreSQL caídos) sí: se detiene con un mensaje claro y, al volver a
arrancarlo, continúa donde iba sin perder ni duplicar eventos.

Uso (con Docker arriba: docker compose up -d --wait):
    python kafka/consumer.py                     escucha hasta Ctrl+C
    python kafka/consumer.py --idle-timeout 15   termina tras 15 s sin mensajes

Código de salida: 0 si terminó bien (Ctrl+C incluido), 1 si se detuvo por una falla.
"""
import argparse
import logging
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException

from happiness_etl import config, db, storage
from happiness_etl.clean import canonical_country
from happiness_etl.contract import ACTUAL_SCORE, PREDICTION_ERROR, VALID, validate_event
from happiness_etl.inference import Predictor

logger = logging.getLogger("consumer")

ALREADY_PROCESSED = "YA_PROCESADO"  # solo para el resumen: reentrega de un evento ya cerrado
MAX_ERROR_LENGTH = 1000


def message_timestamp(message: Any) -> datetime | None:
    """Marca de tiempo que Kafka le puso al mensaje (None si no la tiene)."""
    kind, milliseconds = message.timestamp()
    if kind == 0 or milliseconds is None or milliseconds < 0:  # 0 = TIMESTAMP_NOT_AVAILABLE
        return None
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


def process_message(connection: psycopg.Connection, predictor: Predictor, message: Any) -> str:
    """Procesa un mensaje de punta a punta y devuelve el estado en que quedó.

    Las excepciones de la base de datos se dejan pasar a propósito: son fallas
    de infraestructura y deben detener el consumer (el offset no se confirma).
    """
    where = f"[p{message.partition()} o{message.offset()}]"

    # 1. Crudo primero, antes de interpretar nada
    raw, note = storage.insert_raw(
        connection,
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        key=message.key(),
        timestamp=message_timestamp(message),
        payload=message.value(),
    )
    if not raw.is_new:
        if not raw.is_pending:
            logger.info("%s reentrega de un evento ya procesado (%s): se omite", where, raw.status)
            return ALREADY_PROCESSED
        logger.info("%s evento guardado pero sin procesar (corte anterior): se retoma", where)

    # 2. Contrato
    result = validate_event(message.value())
    if not result.is_valid:
        reason = "; ".join(filter(None, [result.error_message, note]))[:MAX_ERROR_LENGTH]
        storage.mark_invalid(connection, raw.raw_event_id, result.status, reason)
        logger.warning("%s %s: %s", where, result.status, reason)
        return result.status
    event = result.event

    # 3. Predicción (un fallo del modelo es problema de este evento, no del pipeline)
    try:
        predicted = predictor.predict(event)
    except Exception as exc:  # noqa: BLE001 - cualquier fallo del modelo se registra y se sigue
        reason = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
        storage.mark_invalid(connection, raw.raw_event_id, PREDICTION_ERROR, reason)
        logger.error("%s %s: %s", where, PREDICTION_ERROR, reason)
        return PREDICTION_ERROR

    # 4. Predicción + estado VALID, todo o nada
    country = canonical_country(event[config.COUNTRY])
    storage.save_prediction(connection, raw.raw_event_id, event, country, predicted, predictor.version, note)
    logger.info(
        "%s %s %d -> real %.3f, predicho %.3f (error %+.3f)",
        where, country, event[config.YEAR], event[ACTUAL_SCORE], predicted, predicted - event[ACTUAL_SCORE],
    )
    return VALID


class StopFlag:
    """Pedido de parada (Ctrl+C o SIGTERM): se termina el evento en curso y se sale limpio."""

    def __init__(self) -> None:
        self.requested = False

    def request(self, *_: Any) -> None:
        self.requested = True


def consume(
    consumer: Any,
    connection: psycopg.Connection,
    predictor: Predictor,
    topic: str,
    stop: StopFlag,
    idle_timeout: float | None = None,
    counts: Counter | None = None,
    broker_errors: list | None = None,
) -> Counter:
    """Ciclo principal: recibe, procesa y confirma mensaje por mensaje."""
    counts = Counter() if counts is None else counts
    assigned: list = []

    def on_assign(_consumer: Any, partitions: list) -> None:
        assigned.extend(partitions)
        logger.info("Particiones asignadas: %s", sorted(p.partition for p in partitions))

    consumer.subscribe([topic], on_assign=on_assign)
    last_activity: float | None = None
    while not stop.requested:
        message = consumer.poll(1.0)
        if broker_errors:
            raise RuntimeError(f"Se perdió la conexión con Kafka: {broker_errors[0]}")
        if message is None:
            if not assigned:
                # Aún sin particiones: Kafka puede tardar si el consumer anterior murió de
                # golpe (espera a darlo por perdido). El reloj de inactividad no corre todavía.
                continue
            last_activity = last_activity or time.monotonic()
            if idle_timeout is not None and time.monotonic() - last_activity >= idle_timeout:
                logger.info("Sin mensajes nuevos en %s s: se termina", idle_timeout)
                break
            continue
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise RuntimeError(
                    f"El topic '{topic}' no existe. Lo crea 'docker compose up -d --wait' o 'python scripts/reset_demo.py'."
                )
            raise KafkaException(error)
        counts[process_message(connection, predictor, message)] += 1
        # Solo ahora, con todo guardado, se le dice a Kafka que este mensaje ya está atendido
        consumer.commit(message=message, asynchronous=False)
        last_activity = time.monotonic()
    return counts


def _positive_float(text: str) -> float:
    value = float(text)
    if not 0 < value < float("inf"):
        raise argparse.ArgumentTypeError("debe ser un número de segundos mayor que 0")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume eventos de felicidad, predice y guarda en PostgreSQL.")
    parser.add_argument("--idle-timeout", type=_positive_float, default=None,
                        help="terminar tras N segundos sin mensajes (por defecto escucha hasta Ctrl+C)")
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS,
                        help=f"dirección de Kafka (por defecto {config.KAFKA_BOOTSTRAP_SERVERS})")
    parser.add_argument("--topic", default=config.KAFKA_TOPIC, help=f"topic (por defecto {config.KAFKA_TOPIC})")
    parser.add_argument("--group", default=config.KAFKA_CONSUMER_GROUP,
                        help=f"consumer group (por defecto {config.KAFKA_CONSUMER_GROUP})")
    return parser.parse_args(argv)


def create_consumer(bootstrap_servers: str, group: str, broker_errors: list) -> Consumer:
    def on_error(error: KafkaError) -> None:
        # La librería reintenta sola los cortes breves; solo se detiene si no queda ningún broker
        if error.code() == KafkaError._ALL_BROKERS_DOWN or error.fatal():
            broker_errors.append(error.str())

    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group,
        "client.id": "happiness-consumer",
        # El offset se confirma a mano, después de guardar en la base: si el consumer
        # cae a mitad de un evento, Kafka lo vuelve a entregar y no se pierde.
        "enable.auto.commit": False,
        # Un grupo nuevo empieza desde el primer mensaje del topic, no desde "ahora":
        # da igual si el consumer arranca antes o después del producer.
        "auto.offset.reset": "earliest",
        # Si el consumer muere de golpe, Kafka lo da por perdido a los 10 s (por defecto
        # son 45) y le entrega sus particiones al que se arranque después.
        "session.timeout.ms": 10000,
        "log_level": 2,
        "error_cb": on_error,
    })


def main(
    argv: list[str] | None = None,
    connect: Callable[[], psycopg.Connection] = db.connect,
    consumer_factory: Callable[[str, str, list], Any] = create_consumer,
    predictor_factory: Callable[[], Predictor] = Predictor,
    install_signals: bool = True,
) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    try:
        predictor = predictor_factory()
    except (OSError, ValueError) as exc:
        logger.error("No se pudo cargar el modelo (%s). Lo genera notebooks/model_training.ipynb.", exc)
        return 1
    try:
        connection = connect()
    except psycopg.OperationalError as exc:
        logger.error("No hay conexión con PostgreSQL (%s). ¿Está arriba? (docker compose up -d --wait)", str(exc).strip())
        return 1
    connection.autocommit = True  # cada función de storage abre y cierra su propia transacción

    stop = StopFlag()
    if install_signals:
        signal.signal(signal.SIGINT, stop.request)
        signal.signal(signal.SIGTERM, stop.request)

    broker_errors: list = []
    consumer = consumer_factory(args.bootstrap_servers, args.group, broker_errors)
    counts: Counter = Counter()
    started = time.monotonic()
    code = 0
    logger.info(
        "Escuchando '%s' en %s como grupo '%s' (modelo %s). Ctrl+C para detener.",
        args.topic, args.bootstrap_servers, args.group, predictor.version,
    )
    try:
        consume(consumer, connection, predictor, args.topic, stop, args.idle_timeout, counts, broker_errors)
    except psycopg.Error as exc:
        logger.error("Falla de PostgreSQL: %s. El evento en curso no se confirmó: se retomará al reiniciar.", str(exc).strip())
        code = 1
    except (KafkaException, RuntimeError) as exc:
        logger.error("%s", exc)
        code = 1
    finally:
        try:
            consumer.close()  # sale del grupo ordenadamente
        finally:
            connection.close()
    if stop.requested:
        logger.info("Detenido a pedido (Ctrl+C)")
    summary = ", ".join(f"{count} {status}" for status, count in sorted(counts.items())) or "ninguno"
    logger.info("Eventos atendidos en %.1f s: %s", time.monotonic() - started, summary)
    return code


if __name__ == "__main__":
    sys.exit(main())
