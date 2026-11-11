"""Producer de Kafka: transmite el conjunto de prueba como eventos simulados.

Lee data/streaming/happiness_test.csv (el 30 % que el modelo no vio), arma un
evento por fila con la forma del contrato y lo publica en el topic
happiness-events usando el país como llave, uno por uno y con una pausa
configurable entre eventos.

Uso (con Kafka arriba: docker compose up -d --wait):
    python kafka/producer.py                      235 eventos, 0.3 s entre cada uno
    python kafka/producer.py --delay 0 --limit 10 prueba rápida
    python kafka/producer.py --with-invalid       agrega 5 eventos dañados a propósito

Si el topic ya tiene mensajes, avisa (sin bloquear) que reenviar duplicaría las predicciones.
Código de salida: 0 si todo se entregó, 1 si el envío se detuvo por una falla,
130 si se interrumpió con Ctrl+C.
"""
import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from confluent_kafka import KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from happiness_etl import config
from happiness_etl.contract import build_event, serialize_event
from happiness_etl.load import read_processed

logger = logging.getLogger("producer")

DEFAULT_DELAY = 0.3  # segundos entre eventos (a 0.1-0.2 s el producer sigue usando <1 % de CPU)
DELIVERY_TIMEOUT = 15  # segundos que se espera la confirmación de cada evento
TOPIC_WAIT = 30  # segundos que se espera a que el topic exista antes de rendirse

# Fila base de los eventos dañados (los valores del ejemplo del taller)
_INVALID_BASE = {
    "country": "Colombia", "year": 2019, "gdp": 1.2, "family": 0.8, "health": 0.9,
    "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "happiness_score": 6.2,
}


def _without_health(event: dict[str, Any]) -> bytes:
    del event["health"]
    return serialize_event(event)


def _text_in_gdp(event: dict[str, Any]) -> bytes:
    return serialize_event({**event, "gdp": "alto"})


def _truncated_json(event: dict[str, Any]) -> bytes:
    payload = serialize_event(event)
    return payload[: len(payload) // 2]


def _year_2020(event: dict[str, Any]) -> bytes:
    return serialize_event({**event, "year": 2020})


def _negative_gdp(event: dict[str, Any]) -> bytes:
    return serialize_event({**event, "gdp": -1.2})


# Eventos dañados de --with-invalid: (nombre, cómo se daña un evento correcto).
# El consumer debe marcar los tres primeros como INVALID_SCHEMA y los dos últimos
# como INVALID_VALUES, guardarlos en la tabla cruda y seguir sin caerse.
INVALID_CASES: tuple[tuple[str, Callable[[dict[str, Any]], bytes]], ...] = (
    ("falta un campo que el modelo necesita", _without_health),
    ("texto en un campo numérico", _text_in_gdp),
    ("JSON cortado", _truncated_json),
    ("año fuera de 2015-2019", _year_2020),
    ("valor negativo", _negative_gdp),
)


@dataclass(frozen=True)
class PlannedEvent:
    """Un evento por enviar: la fila de origen y, si es de demostración, cómo dañarlo."""

    record: dict[str, Any]
    damage_name: str | None = None
    damage: Callable[[dict[str, Any]], bytes] | None = None

    def render(self) -> tuple[bytes, bytes, dict[str, Any]]:
        """Devuelve (llave, valor, evento). event_time se sella aquí: es el momento de la emisión."""
        event = build_event(self.record)
        value = self.damage(dict(event)) if self.damage else serialize_event(event)
        return str(self.record[config.COUNTRY]).encode("utf-8"), value, event


def load_records(path: Path, seed: int, limit: int | None = None) -> list[dict[str, Any]]:
    """Lee el archivo de streaming y devuelve sus filas mezcladas de forma reproducible.

    Se mezclan (misma semilla = mismo orden) para que lleguen países y años
    variados, como en un flujo real, y no un año completo tras otro.
    """
    frame = read_processed(path).sample(frac=1, random_state=seed)
    if limit is not None:
        frame = frame.head(limit)
    return frame.to_dict("records")


def build_plan(records: list[dict[str, Any]], with_invalid: bool = False) -> list[PlannedEvent]:
    """Orden final de envío. Los eventos dañados se reparten a intervalos fijos entre los buenos."""
    plan = [PlannedEvent(record) for record in records]
    if with_invalid:
        step = len(records) / (len(INVALID_CASES) + 1)
        # De atrás hacia adelante para que insertar uno no mueva la posición de los demás
        for index in reversed(range(len(INVALID_CASES))):
            name, damage = INVALID_CASES[index]
            plan.insert(round(step * (index + 1)), PlannedEvent(dict(_INVALID_BASE), name, damage))
    return plan


def check_topic(producer: Producer, topic: str) -> None:
    """Falla con un mensaje claro si Kafka no responde o el topic no existe.

    Si el topic aún no aparece se espera un poco: justo después de
    'docker compose up -d --wait' el servicio que lo crea puede tardar unos segundos.
    """
    deadline = time.monotonic() + TOPIC_WAIT
    announced = False
    while True:
        try:
            metadata = producer.list_topics(topic, timeout=10)
        except KafkaException as exc:
            raise RuntimeError(
                f"No hay conexión con Kafka: {exc.args[0].str()}. ¿Está arriba? (docker compose up -d --wait)"
            ) from exc
        found = metadata.topics.get(topic)
        if found is not None and found.error is None:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"El topic '{topic}' no existe (la creación automática está desactivada). "
                "Lo crea 'docker compose up -d --wait' o 'python scripts/reset_demo.py'."
            )
        if not announced:
            logger.info("El topic '%s' aún no existe: se espera hasta %d s a que se cree", topic, TOPIC_WAIT)
            announced = True
        time.sleep(1)
    if len(found.partitions) != config.KAFKA_TOPIC_PARTITIONS:
        logger.warning(
            "El topic tiene %d particiones y el taller pide %d", len(found.partitions), config.KAFKA_TOPIC_PARTITIONS
        )


def count_messages(bootstrap_servers: str, topic: str) -> int:
    """Cuántos mensajes tiene hoy el topic (último offset menos primero, sumado por partición)."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers, "log_level": 2})
    partitions = admin.list_topics(topic, timeout=10).topics[topic].partitions

    def total(spec: OffsetSpec) -> int:
        futures = admin.list_offsets({TopicPartition(topic, p): spec for p in partitions}, request_timeout=10)
        return sum(future.result().offset for future in futures.values())

    return total(OffsetSpec.latest()) - total(OffsetSpec.earliest())


def warn_if_topic_has_messages(counter: Callable[[str, str], int], bootstrap_servers: str, topic: str) -> None:
    """Avisa (sin bloquear) si el topic ya tiene mensajes: reenviar duplicaría las predicciones.

    Cada corrida genera event_id nuevos, así que si el consumer ya procesó los
    mensajes anteriores, los nuevos se sumarían a los mismos países y años.
    """
    try:
        existing = counter(bootstrap_servers, topic)
    except KafkaException as exc:  # es solo un aviso: si no se puede contar, se sigue
        logger.debug("No se pudo contar los mensajes del topic: %s", exc)
        return
    if existing:
        logger.warning(
            "El topic '%s' ya tiene %d mensajes: si el consumer ya los procesó, estos eventos se duplicarán "
            "en la base. 'python scripts/reset_demo.py' deja todo en cero.",
            topic, existing,
        )


@dataclass
class SendStats:
    """Conteo del envío. Se va actualizando para poder informar aunque se interrumpa."""

    total: int
    delivered: int = 0


def send_all(producer: Producer, topic: str, plan: list[PlannedEvent], delay: float, stats: SendStats) -> None:
    """Envía los eventos uno por uno, en orden.

    Tras cada envío se espera la confirmación del broker antes de seguir: así el
    registro muestra la partición y el offset de cada evento. Si un evento no se
    puede entregar se detiene el envío (RuntimeError) en vez de seguir a ciegas.
    """
    for number, planned in enumerate(plan, start=1):
        key, value, event = planned.render()
        outcome: dict[str, Any] = {}

        def on_delivery(error, message, outcome=outcome):
            outcome["error"] = error
            outcome["message"] = message

        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
        producer.flush(DELIVERY_TIMEOUT + 5)

        label = f"[{number:>3}/{stats.total}] {event['country']} {event['year']}"
        if planned.damage_name:
            label += f" (DAÑADO A PROPÓSITO: {planned.damage_name})"
        if outcome.get("message") is None:
            raise RuntimeError(f"{label} -> Kafka no confirmó la entrega: se detiene el envío")
        if outcome["error"] is not None:
            raise RuntimeError(f"{label} -> no entregado ({outcome['error'].str()}): se detiene el envío")
        stats.delivered += 1
        message = outcome["message"]
        logger.info(
            "%s -> partición %d, offset %d, event_id %s",
            label, message.partition(), message.offset(), event["event_id"],
        )
        if delay and number < stats.total:
            time.sleep(delay)


def _positive_int(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("debe ser mayor que 0")
    return value


def _non_negative_float(text: str) -> float:
    value = float(text)
    if not 0 <= value < float("inf"):
        raise argparse.ArgumentTypeError("debe ser un número de segundos mayor o igual a 0")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publica el conjunto de prueba en Kafka como eventos simulados.")
    parser.add_argument("--delay", type=_non_negative_float, default=DEFAULT_DELAY,
                        help=f"segundos de pausa entre eventos (por defecto {DEFAULT_DELAY})")
    parser.add_argument("--limit", type=_positive_int, default=None,
                        help="enviar solo los primeros N registros (por defecto todos)")
    parser.add_argument("--with-invalid", action="store_true",
                        help=f"intercalar {len(INVALID_CASES)} eventos dañados a propósito para mostrar la validación")
    parser.add_argument("--seed", type=int, default=config.RANDOM_STATE,
                        help=f"semilla del orden de envío (por defecto {config.RANDOM_STATE})")
    parser.add_argument("--input", type=Path, default=config.STREAMING_FILE,
                        help="CSV con el esquema unificado (por defecto el conjunto de prueba)")
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS,
                        help=f"dirección de Kafka (por defecto {config.KAFKA_BOOTSTRAP_SERVERS})")
    parser.add_argument("--topic", default=config.KAFKA_TOPIC, help=f"topic (por defecto {config.KAFKA_TOPIC})")
    return parser.parse_args(argv)


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "happiness-producer",
        # Idempotencia: el broker confirma cada evento (acks=all) y descarta los
        # duplicados de un reintento, así que no se pierde ni se repite ninguno.
        "enable.idempotence": True,
        "message.timeout.ms": DELIVERY_TIMEOUT * 1000,
        # Solo mensajes críticos de la librería: si Kafka no responde, ella repite el
        # mismo error varias veces por segundo; aquí se informa una sola vez y claro.
        "log_level": 2,
    })


def main(
    argv: list[str] | None = None,
    producer_factory: Callable[[str], Any] = create_producer,
    message_counter: Callable[[str, str], int] = count_messages,
) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    try:
        records = load_records(args.input, args.seed, args.limit)
    except FileNotFoundError:
        logger.error("No existe el archivo %s (lo genera notebooks/model_training.ipynb)", args.input)
        return 1
    plan = build_plan(records, args.with_invalid)
    stats = SendStats(total=len(plan))
    producer = producer_factory(args.bootstrap_servers)
    started = time.monotonic()
    code = 0
    try:
        check_topic(producer, args.topic)
        warn_if_topic_has_messages(message_counter, args.bootstrap_servers, args.topic)
        logger.info(
            "Enviando %d eventos a '%s' en %s (pausa de %s s)", stats.total, args.topic, args.bootstrap_servers, args.delay
        )
        send_all(producer, args.topic, plan, args.delay, stats)
    except RuntimeError as exc:
        logger.error("%s", exc)
        code = 1
    except KeyboardInterrupt:
        producer.flush(DELIVERY_TIMEOUT)  # se entrega lo que ya estaba en camino
        logger.warning("Interrumpido con Ctrl+C")
        code = 130
    logger.info(
        "%s: %d de %d eventos entregados en %.1f s",
        "Listo" if code == 0 else "Envío incompleto", stats.delivered, stats.total, time.monotonic() - started,
    )
    return code


if __name__ == "__main__":
    sys.exit(main())
