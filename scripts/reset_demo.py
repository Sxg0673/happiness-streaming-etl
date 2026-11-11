"""Deja la demo en cero: sin eventos, sin predicciones y con el topic vacío.

Kafka y PostgreSQL guardan sus datos en volúmenes, así que entre una corrida y
otra todo se acumula. Este script borra SOLO los datos del streaming:

  1. Vacía fact_predictions y raw_happiness_events (las dimensiones no se tocan).
  2. Borra el consumer group happiness-ml-team (sus offsets guardados).
  3. Borra y vuelve a crear el topic happiness-events (3 particiones).

Las tres cosas van juntas a propósito: si se recreara el topic sin vaciar las
tablas, los offsets volverían a empezar en 0 y chocarían con los ya guardados.
No borra volúmenes de Docker ni el esquema.

Uso (con el consumer detenido y Docker arriba):
    python scripts/reset_demo.py          pide confirmación
    python scripts/reset_demo.py --yes    sin preguntar
"""
import argparse
import sys
import time

import psycopg
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from happiness_etl import config, db

TIMEOUT = 30  # segundos de espera máxima por operación


def active_members(admin: AdminClient, group: str) -> int:
    """Cuántos consumers del grupo están conectados ahora (0 si el grupo no existe)."""
    description = admin.describe_consumer_groups([group], request_timeout=TIMEOUT)[group].result()
    return len(description.members)


def count_rows(connection: psycopg.Connection) -> tuple[int, int]:
    """Filas actuales en (raw_happiness_events, fact_predictions)."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT (SELECT count(*) FROM raw_happiness_events), (SELECT count(*) FROM fact_predictions)")
        return cursor.fetchone()


def truncate_tables(connection: psycopg.Connection) -> None:
    """Vacía hechos y eventos crudos en una sola transacción y reinicia sus contadores."""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE fact_predictions, raw_happiness_events RESTART IDENTITY")
    connection.commit()


def _partition_count(admin: AdminClient, topic: str) -> int | None:
    found = admin.list_topics(timeout=TIMEOUT).topics.get(topic)
    return len(found.partitions) if found is not None and found.error is None else None


def recreate_topic(admin: AdminClient, topic: str, partitions: int) -> bool:
    """Borra el topic (si existe) y lo crea de nuevo. Devuelve True si existía."""
    existed = True
    try:
        admin.delete_topics([topic], operation_timeout=TIMEOUT)[topic].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise
        existed = False
    deadline = time.monotonic() + TIMEOUT
    while True:  # el borrado es asíncrono: crear puede fallar un instante con "ya existe"
        try:
            new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=1)
            admin.create_topics([new_topic], operation_timeout=TIMEOUT)[topic].result()
            break
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS or time.monotonic() > deadline:
                raise
            time.sleep(0.5)
    while _partition_count(admin, topic) != partitions:  # hasta que el broker lo anuncie
        if time.monotonic() > deadline:
            raise RuntimeError(f"El topic '{topic}' no quedó disponible en {TIMEOUT} s")
        time.sleep(0.5)
    return existed


def delete_group(admin: AdminClient, group: str) -> bool:
    """Borra el consumer group. Devuelve False si no existía."""
    try:
        admin.delete_consumer_groups([group], request_timeout=TIMEOUT)[group].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND:
            raise
        return False
    return True


def confirm() -> bool:
    if not sys.stdin.isatty():
        print("Sin terminal para confirmar: repetir con --yes.")
        return False
    answer = input("Se borrarán los eventos, las predicciones y el contenido del topic. ¿Continuar? [s/N] ")
    return answer.strip().lower() in ("s", "si", "sí", "y", "yes")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deja la demo en cero (eventos, predicciones, topic y consumer group).")
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    args = parser.parse_args(argv)

    topic, group = config.KAFKA_TOPIC, config.KAFKA_CONSUMER_GROUP
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    try:
        # Primero se comprueba que todo responde: no se borra nada a medias
        members = active_members(admin, group)
        connection = db.connect()
    except (KafkaException, psycopg.OperationalError) as exc:
        print(f"No se pudo conectar ({exc}). ¿Docker está arriba? (docker compose up -d --wait)")
        return 1

    with connection:
        if members:
            print(f"El consumer group '{group}' tiene {members} consumer(s) activo(s): detener el consumer (Ctrl+C) y repetir.")
            return 1
        raw_rows, fact_rows = count_rows(connection)
        print(f"Estado actual: {raw_rows} eventos crudos y {fact_rows} predicciones en la base.")
        if not (args.yes or confirm()):
            print("Cancelado: no se borró nada.")
            return 1

        truncate_tables(connection)
        print("1/3 Tablas raw_happiness_events y fact_predictions vacías (dimensiones intactas).")
        deleted = delete_group(admin, group)
        print(f"2/3 Consumer group '{group}' {'borrado' if deleted else 'no existía'}.")
        existed = recreate_topic(admin, topic, config.KAFKA_TOPIC_PARTITIONS)
        print(f"3/3 Topic '{topic}' {'recreado' if existed else 'creado'} con {config.KAFKA_TOPIC_PARTITIONS} particiones.")
    print("Listo: la demo está en cero.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
