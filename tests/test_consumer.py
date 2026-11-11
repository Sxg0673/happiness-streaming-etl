"""Pruebas de integración del consumer contra PostgreSQL y Kafka reales.

Se saltan solas si Docker no está arriba (docker compose up -d --wait).
No tocan los datos de la demo: usan un esquema temporal de PostgreSQL y, para
Kafka, un topic y un consumer group temporales que se borran al terminar.
"""
import importlib.util
import json
import uuid
from collections import Counter
from types import SimpleNamespace

import psycopg
import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from happiness_etl import config, db
from happiness_etl.contract import (
    INVALID_SCHEMA,
    INVALID_VALUES,
    PREDICTION_ERROR,
    RECEIVED,
    VALID,
    build_event,
    serialize_event,
)
from happiness_etl.inference import Predictor
from happiness_etl.load import read_processed
from happiness_etl import storage


def _load(name):
    spec = importlib.util.spec_from_file_location(name, config.ROOT_DIR / "kafka" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


consumer = _load("consumer")
producer = _load("producer")

ROW = {"country": "Colombia", "year": 2019, "gdp": 1.2, "family": 0.8, "health": 0.9,
       "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "happiness_score": 6.2}


class FakeMessage:
    """Mensaje de Kafka mínimo para process_message."""

    def __init__(self, value, offset, key=b"Colombia", partition=0, topic="prueba", timestamp=(1, 1794358507006)):
        self._data = (value, offset, key, partition, topic, timestamp)

    def value(self): return self._data[0]
    def offset(self): return self._data[1]
    def key(self): return self._data[2]
    def partition(self): return self._data[3]
    def topic(self): return self._data[4]
    def timestamp(self): return self._data[5]
    def error(self): return None


def message(offset=0, **changes):
    """Mensaje válido (Colombia 2019) con los campos cambiados que se pidan."""
    return FakeMessage(serialize_event({**build_event(ROW), **changes}), offset)


@pytest.fixture(scope="module")
def schema_name():
    """Esquema temporal con las tablas y dimensiones del proyecto."""
    try:
        admin = db.connect(connect_timeout=3, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL no está disponible (docker compose up -d --wait)")
    name = f"test_{uuid.uuid4().hex[:10]}"
    admin.execute(f"CREATE SCHEMA {name}")
    try:
        with db.connect(options=f"-c search_path={name}", autocommit=True) as setup:
            for script in ("create_tables.sql", "seed_dimensions.sql"):
                setup.execute((config.ROOT_DIR / "sql" / script).read_text(encoding="utf-8"))
        yield name
    finally:
        admin.execute(f"DROP SCHEMA {name} CASCADE")
        admin.close()


@pytest.fixture
def conn(schema_name):
    """Conexión al esquema temporal, con las tablas de eventos vacías en cada prueba."""
    with db.connect(options=f"-c search_path={schema_name}", autocommit=True) as connection:
        connection.execute("TRUNCATE fact_predictions, raw_happiness_events RESTART IDENTITY")
        connection.execute("DELETE FROM dim_country WHERE region IS NULL")
        yield connection


@pytest.fixture(scope="module")
def predictor():
    return Predictor()


def one(conn, sql, *params):
    return conn.execute(sql, params).fetchone()


# --- process_message ----------------------------------------------------------

def test_valid_event_is_stored_raw_and_predicted(conn, predictor):
    msg = message(offset=7)
    assert consumer.process_message(conn, predictor, msg) == VALID
    raw = one(conn, "SELECT raw_event_id, kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload, status, "
                    "error_message, processed_at >= received_at, kafka_timestamp IS NOT NULL FROM raw_happiness_events")
    assert raw[1:5] == ("prueba", 0, 7, "Colombia") and raw[5].encode("utf-8") == msg.value()
    assert raw[6:] == (VALID, None, True, True)
    fact = one(conn, "SELECT f.raw_event_id, f.event_id, c.country_name, f.date_id, f.actual_score, f.predicted_score, "
                     "f.prediction_error, f.absolute_error, f.model_version, f.prediction_timestamp >= f.event_time "
                     "FROM fact_predictions f JOIN dim_country c USING (country_id)")
    event = json.loads(msg.value())
    assert fact[:5] == (raw[0], event["event_id"], "Colombia", 2019, 6.2)
    assert fact[5] == pytest.approx(predictor.predict(event))
    assert fact[6] == pytest.approx(fact[5] - 6.2) and fact[7] == pytest.approx(abs(fact[6]))
    assert fact[8] == predictor.version and fact[9] is True


@pytest.mark.parametrize(
    "changes, status, reason",
    [
        ({"year": 2020}, INVALID_VALUES, "year 2020 fuera"),
        ({"gdp": -1.2}, INVALID_VALUES, "negativo"),
        ({"gdp": 500}, INVALID_VALUES, "supera el máximo"),
        ({"gdp": "alto"}, INVALID_SCHEMA, "gdp debe ser un número"),
        ({"year": "2019"}, INVALID_SCHEMA, "year debe ser un entero"),
        ({"event_time": "ayer"}, INVALID_SCHEMA, "event_time"),
    ],
)
def test_invalid_events_are_stored_marked_and_skipped(conn, predictor, changes, status, reason):
    assert consumer.process_message(conn, predictor, message(**changes)) == status
    assert one(conn, "SELECT status, error_message LIKE %s, processed_at IS NOT NULL FROM raw_happiness_events",
               f"%{reason}%") == (status, True, True)
    assert one(conn, "SELECT count(*) FROM fact_predictions") == (0,)


@pytest.mark.parametrize(
    "payload, stored, reason",
    [
        (b'{"event_id": "E1", "country": "Col', '{"event_id": "E1", "country": "Col', "JSON mal formado"),
        (b"\xff\xfe no es texto", "�� no es texto", "no es UTF-8"),
        (b'{"a": "\x00"}', '{"a": "�"}', "nulos"),
        (None, "", "sin contenido"),
        (b"[1, 2]", "[1, 2]", "no es un objeto JSON"),
    ],
)
def test_unreadable_messages_do_not_break_the_consumer(conn, predictor, payload, stored, reason):
    assert consumer.process_message(conn, predictor, FakeMessage(payload, 0, key=None)) == INVALID_SCHEMA
    assert one(conn, "SELECT raw_payload, status, error_message LIKE %s, kafka_key FROM raw_happiness_events",
               f"%{reason}%") == (stored, INVALID_SCHEMA, True, None)
    # y el siguiente evento se procesa con normalidad
    assert consumer.process_message(conn, predictor, message(offset=1)) == VALID


def test_missing_field_is_invalid_schema_with_every_reason(conn, predictor):
    event = {k: v for k, v in build_event(ROW).items() if k not in ("health", "gdp")}
    assert consumer.process_message(conn, predictor, FakeMessage(serialize_event(event), 0)) == INVALID_SCHEMA
    assert one(conn, "SELECT error_message FROM raw_happiness_events") == ("falta el campo 'gdp'; falta el campo 'health'",)


def test_country_variants_use_the_existing_dimension_row(conn, predictor):
    before = one(conn, "SELECT count(*) FROM dim_country")
    assert consumer.process_message(conn, predictor, message(country="  Trinidad &  Tobago ")) == VALID
    assert one(conn, "SELECT c.country_name, c.region FROM fact_predictions JOIN dim_country c USING (country_id)") == (
        "Trinidad and Tobago", "Latin America and Caribbean")
    assert one(conn, "SELECT count(*) FROM dim_country") == before


def test_unknown_country_is_added_without_region(conn, predictor):
    for offset in (0, 1):  # dos eventos del mismo país nuevo: una sola fila en la dimensión
        assert consumer.process_message(conn, predictor, message(offset, country="Narnia")) == VALID
    assert conn.execute("SELECT country_name, region FROM dim_country WHERE region IS NULL").fetchall() == [("Narnia", None)]
    assert one(conn, "SELECT count(*), count(DISTINCT country_id) FROM fact_predictions") == (2, 1)


def test_redelivered_message_is_not_duplicated(conn, predictor):
    msg = message(offset=3)
    assert consumer.process_message(conn, predictor, msg) == VALID
    assert consumer.process_message(conn, predictor, msg) == consumer.ALREADY_PROCESSED
    assert one(conn, "SELECT (SELECT count(*) FROM raw_happiness_events), (SELECT count(*) FROM fact_predictions)") == (1, 1)


def test_event_left_as_received_is_resumed(conn, predictor):
    """Si el consumer cayó justo después de guardar el crudo, al reentregarse se termina de procesar."""
    msg = message(offset=4)
    raw, _ = storage.insert_raw(conn, topic=msg.topic(), partition=0, offset=4, key=msg.key(), timestamp=None,
                                payload=msg.value())
    assert raw.is_new and raw.status == RECEIVED
    assert one(conn, "SELECT status, processed_at FROM raw_happiness_events") == (RECEIVED, None)
    assert consumer.process_message(conn, predictor, msg) == VALID
    assert one(conn, "SELECT count(*), min(status) FROM raw_happiness_events") == (1, VALID)
    assert one(conn, "SELECT raw_event_id FROM fact_predictions") == (raw.raw_event_id,)


def test_model_failure_is_recorded_and_the_next_event_continues(conn, predictor):
    class BrokenModel:
        version = "roto"

        def predict(self, event):
            raise ValueError("categoría desconocida")

    assert consumer.process_message(conn, BrokenModel(), message(offset=0)) == PREDICTION_ERROR
    assert one(conn, "SELECT status, error_message FROM raw_happiness_events") == (
        PREDICTION_ERROR, "ValueError: categoría desconocida")
    assert one(conn, "SELECT count(*) FROM fact_predictions") == (0,)
    assert consumer.process_message(conn, predictor, message(offset=1)) == VALID


def test_database_failure_is_not_swallowed(conn, predictor):
    """Una falla de la base es de infraestructura: debe salir para que el consumer se detenga."""
    conn.execute("ALTER TABLE fact_predictions RENAME TO fact_oculta")
    try:
        with pytest.raises(psycopg.Error):
            consumer.process_message(conn, predictor, message(offset=9))
        # el crudo quedó guardado y pendiente: se retomará al reiniciar
        assert one(conn, "SELECT status FROM raw_happiness_events") == (RECEIVED,)
    finally:
        conn.execute("ALTER TABLE fact_oculta RENAME TO fact_predictions")
    assert consumer.process_message(conn, predictor, message(offset=9)) == VALID


def test_all_streaming_records_give_the_training_metrics_in_sql(conn, predictor):
    rows = read_processed(config.STREAMING_FILE).to_dict("records")
    for offset, row in enumerate(rows):
        msg = FakeMessage(serialize_event(build_event(row)), offset, key=row["country"].encode("utf-8"))
        assert consumer.process_message(conn, predictor, msg) == VALID
    expected = json.loads(config.METRICS_FILE.read_text(encoding="utf-8"))["test_metrics"]
    mae, rmse, bias, n = one(conn, "SELECT round(avg(absolute_error)::numeric, 4), round(sqrt(avg(prediction_error ^ 2))::numeric, 4), "
                                   "round(avg(prediction_error)::numeric, 4), count(*) FROM fact_predictions")
    assert (float(mae), float(rmse), float(bias), n) == (expected["mae"], expected["rmse"], expected["bias"], 235)


# --- Ciclo consume con un Kafka falso ------------------------------------------

class FakeConsumer:
    def __init__(self, items, stop=None, stop_after=None, assign=True):
        self.items, self.stop, self.stop_after, self.assign = list(items), stop, stop_after, assign
        self.log, self.polls = [], 0

    def subscribe(self, topics, on_assign=None):
        self.log.append(("subscribe", topics))
        if self.assign:
            on_assign(self, [SimpleNamespace(partition=0)])

    def poll(self, timeout):
        self.polls += 1
        if self.stop is not None and len([e for e in self.log if e[0] == "commit"]) == self.stop_after:
            self.stop.request()
        if self.stop is not None and self.stop_after is None and self.polls >= 5:
            self.stop.request()
        return self.items.pop(0) if self.items else None

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.log.append(("commit", message.offset()))


class ErrorMessage:
    def __init__(self, code):
        self._code = code

    def error(self):
        return KafkaError(self._code)


def test_consume_commits_each_offset_after_storing_it(conn, predictor):
    stop = consumer.StopFlag()
    fake = FakeConsumer([message(0), message(1, year=2020), message(2)], stop, stop_after=3)
    counts = consumer.consume(fake, conn, predictor, "prueba", stop)
    assert counts == Counter({VALID: 2, INVALID_VALUES: 1})
    assert fake.log == [("subscribe", ["prueba"]), ("commit", 0), ("commit", 1), ("commit", 2)]


def test_stop_request_finishes_cleanly_without_touching_pending_messages(conn, predictor):
    stop = consumer.StopFlag()
    fake = FakeConsumer([message(0), message(1), message(2)], stop, stop_after=1)
    consumer.consume(fake, conn, predictor, "prueba", stop)
    # el pedido de parada llegó tras confirmar el primero; el que ya estaba en curso se termina
    assert one(conn, "SELECT count(*), min(status), max(status) FROM raw_happiness_events") == (2, VALID, VALID)
    assert len(fake.items) == 1


def test_idle_timeout_ends_the_loop(conn, predictor):
    counts = consumer.consume(FakeConsumer([]), conn, predictor, "prueba", consumer.StopFlag(), idle_timeout=0.01)
    assert counts == Counter()


def test_idle_timeout_does_not_run_before_partitions_are_assigned(conn, predictor):
    """Si el consumer anterior murió de golpe, Kafka tarda en asignar: no hay que rendirse antes."""
    stop = consumer.StopFlag()
    fake = FakeConsumer([], stop, assign=False)
    consumer.consume(fake, conn, predictor, "prueba", stop, idle_timeout=0.001)
    assert fake.polls == 5 and stop.requested  # siguió esperando hasta que se pidió parar


def test_database_failure_leaves_the_offset_uncommitted(conn, predictor):
    conn.execute("ALTER TABLE fact_predictions RENAME TO fact_oculta")
    fake = FakeConsumer([message(0)])
    try:
        with pytest.raises(psycopg.Error):
            consumer.consume(fake, conn, predictor, "prueba", consumer.StopFlag())
    finally:
        conn.execute("ALTER TABLE fact_oculta RENAME TO fact_predictions")
    assert ("commit", 0) not in fake.log


def test_missing_topic_and_lost_broker_stop_with_a_clear_error(conn, predictor):
    with pytest.raises(RuntimeError, match="no existe"):
        consumer.consume(FakeConsumer([ErrorMessage(KafkaError.UNKNOWN_TOPIC_OR_PART)]), conn, predictor, "x",
                         consumer.StopFlag())
    with pytest.raises(RuntimeError, match="conexión con Kafka"):
        consumer.consume(FakeConsumer([]), conn, predictor, "x", consumer.StopFlag(), broker_errors=["sin brokers"])
    with pytest.raises(KafkaException):
        consumer.consume(FakeConsumer([ErrorMessage(KafkaError._FATAL)]), conn, predictor, "x", consumer.StopFlag())


def test_main_reports_a_database_that_is_down(caplog):
    def no_database():
        raise psycopg.OperationalError("connection refused")

    assert consumer.main([], connect=no_database, install_signals=False) == 1
    assert "No hay conexión con PostgreSQL" in caplog.text


# --- De punta a punta con Kafka real -------------------------------------------

@pytest.fixture
def kafka_names():
    """Topic y consumer group temporales (se borran al terminar)."""
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "log_level": 2})
    try:
        admin.list_topics(timeout=3)
    except KafkaException:
        pytest.skip("Kafka no está disponible (docker compose up -d --wait)")
    suffix = uuid.uuid4().hex[:8]
    topic, group = f"prueba-consumer-{suffix}", f"prueba-grupo-{suffix}"
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)], operation_timeout=20)[topic].result()
    try:
        yield topic, group
    finally:
        for futures in (admin.delete_consumer_groups([group], request_timeout=20), admin.delete_topics([topic], operation_timeout=20)):
            for future in futures.values():
                try:
                    future.result()
                except KafkaException:
                    pass


def test_producer_to_database_end_to_end(conn, schema_name, kafka_names):
    topic, group = kafka_names
    connect = lambda: db.connect(options=f"-c search_path={schema_name}")  # noqa: E731
    run = lambda: consumer.main(["--idle-timeout", "6", "--topic", topic, "--group", group],  # noqa: E731
                                connect=connect, install_signals=False)

    assert producer.main(["--delay", "0", "--limit", "30", "--with-invalid", "--topic", topic]) == 0
    assert run() == 0
    by_status = dict(conn.execute("SELECT status, count(*) FROM raw_happiness_events GROUP BY 1").fetchall())
    assert by_status == {VALID: 30, INVALID_SCHEMA: 3, INVALID_VALUES: 2}
    assert one(conn, "SELECT count(*), count(DISTINCT raw_event_id), count(DISTINCT event_id) FROM fact_predictions") == (30, 30, 30)
    # la llave de Kafka es el país y cada país cae en una sola partición
    assert one(conn, "SELECT count(*) FROM (SELECT kafka_key FROM raw_happiness_events GROUP BY 1 "
                     "HAVING count(DISTINCT kafka_partition) > 1) t") == (0,)
    assert one(conn, "SELECT bool_and(c.country_name = r.kafka_key) FROM fact_predictions f "
                     "JOIN raw_happiness_events r USING (raw_event_id) JOIN dim_country c USING (country_id)") == (True,)

    # Segunda corrida del consumer: los offsets quedaron confirmados, no hay nada nuevo
    assert run() == 0
    assert one(conn, "SELECT (SELECT count(*) FROM raw_happiness_events), (SELECT count(*) FROM fact_predictions)") == (35, 30)

    # Llegan más eventos: solo se procesan los nuevos
    assert producer.main(["--delay", "0", "--limit", "5", "--topic", topic]) == 0
    assert run() == 0
    assert one(conn, "SELECT (SELECT count(*) FROM raw_happiness_events), (SELECT count(*) FROM fact_predictions)") == (40, 35)
