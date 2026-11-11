"""Pruebas del producer sin Kafka: orden de envío, eventos dañados y flujo con un broker falso."""
import importlib.util
import json
import zlib
from collections import Counter
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from happiness_etl import config
from happiness_etl.contract import INVALID_SCHEMA, INVALID_VALUES, VALID, validate_event

# kafka/ no es un paquete (así no se confunde con ninguna librería): se carga por ruta
_spec = importlib.util.spec_from_file_location("producer", config.ROOT_DIR / "kafka" / "producer.py")
producer = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(producer)


class FakeMessage:
    def __init__(self, partition, offset):
        self._partition, self._offset = partition, offset

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeKafka:
    """Broker falso: guarda lo producido y confirma cada mensaje al hacer flush."""

    def __init__(self, topics=(config.KAFKA_TOPIC,), partitions=3, fail_at=None):
        self.topics, self.partitions, self.fail_at = topics, partitions, fail_at
        self.sent, self._pending, self._offsets = [], [], Counter()

    def list_topics(self, topic, timeout):
        found = SimpleNamespace(error=None, partitions=dict.fromkeys(range(self.partitions)))
        return SimpleNamespace(topics={name: found for name in self.topics})

    def produce(self, topic, key, value, on_delivery):
        self._pending.append((topic, key, value, on_delivery))

    def flush(self, timeout=None):
        for topic, key, value, on_delivery in self._pending:
            if self.fail_at is not None and len(self.sent) + 1 == self.fail_at:
                on_delivery(SimpleNamespace(str=lambda: "broker caído"), FakeMessage(None, None))
                continue
            partition = zlib.crc32(key) % self.partitions
            self.sent.append((topic, key, value, partition))
            on_delivery(None, FakeMessage(partition, self._offsets[partition]))
            self._offsets[partition] += 1
        self._pending.clear()
        return 0


def run(argv, fake, existing=0, counter=None):
    """Corre main con el broker falso; 'existing' = mensajes que el topic ya tendría."""
    counter = counter or (lambda servers, topic: existing)
    return producer.main(["--delay", "0", *argv], producer_factory=lambda servers: fake, message_counter=counter)


# --- Orden de envío ----------------------------------------------------------

def test_load_records_is_complete_shuffled_and_reproducible():
    records = producer.load_records(config.STREAMING_FILE, seed=42)
    again = producer.load_records(config.STREAMING_FILE, seed=42)
    other = producer.load_records(config.STREAMING_FILE, seed=7)
    pairs = [(r["country"], r["year"]) for r in records]
    assert len(records) == 235 and len(set(pairs)) == 235
    assert records == again and records != other
    assert pairs != sorted(pairs, key=lambda p: (p[1], p[0]))  # no llega un año tras otro
    assert len({r["year"] for r in records[:20]}) > 1
    assert type(records[0]["year"]) is int and type(records[0]["gdp"]) is float


def test_limit_takes_the_first_records_of_the_same_order():
    full = producer.load_records(config.STREAMING_FILE, seed=42)
    assert producer.load_records(config.STREAMING_FILE, seed=42, limit=10) == full[:10]


# --- Eventos dañados ---------------------------------------------------------

def test_plan_without_invalid_events_is_only_the_records():
    records = producer.load_records(config.STREAMING_FILE, seed=42, limit=10)
    plan = producer.build_plan(records)
    assert [p.record for p in plan] == records and all(p.damage is None for p in plan)


@pytest.mark.parametrize("limit, positions", [(None, [39, 79, 120, 160, 200]), (10, [2, 4, 7, 10, 12]), (1, [0, 1, 2, 4, 5])])
def test_invalid_events_are_spread_at_fixed_positions(limit, positions):
    records = producer.load_records(config.STREAMING_FILE, seed=42, limit=limit)
    plan = producer.build_plan(records, with_invalid=True)
    assert len(plan) == len(records) + 5
    assert [i for i, p in enumerate(plan) if p.damage] == positions
    assert [p.damage_name for p in plan if p.damage] == [name for name, _ in producer.INVALID_CASES]
    assert [p.record for p in plan if not p.damage] == records  # los buenos conservan su orden


def test_each_invalid_case_gets_the_expected_status():
    plan = [p for p in producer.build_plan([], with_invalid=True)]
    statuses = [validate_event(p.render()[1]).status for p in plan]
    assert statuses == [INVALID_SCHEMA, INVALID_SCHEMA, INVALID_SCHEMA, INVALID_VALUES, INVALID_VALUES]
    assert all(validate_event(p.render()[1]).error_message for p in plan)


def test_render_uses_country_as_key_and_fresh_ids():
    planned = producer.PlannedEvent({**producer._INVALID_BASE, "country": "Côte d'Ivoire"})
    key, value, event = planned.render()
    assert key == "Côte d'Ivoire".encode("utf-8")
    assert json.loads(value) == event and validate_event(value).is_valid
    assert planned.render()[2]["event_id"] != event["event_id"]


# --- Flujo completo con el broker falso ---------------------------------------

def test_full_run_sends_every_record_once_with_valid_events(caplog):
    fake = FakeKafka()
    with caplog.at_level("INFO", logger="producer"):
        assert run([], fake) == 0
    assert len(fake.sent) == 235 and {topic for topic, *_ in fake.sent} == {config.KAFKA_TOPIC}
    events = [json.loads(value) for _, _, value, _ in fake.sent]
    assert all(validate_event(value).is_valid for _, _, value, _ in fake.sent)
    assert len({e["event_id"] for e in events}) == 235
    assert [key for _, key, _, _ in fake.sent] == [e["country"].encode("utf-8") for e in events]
    # El mismo país siempre cae en la misma partición
    by_country = {}
    for _, key, _, partition in fake.sent:
        assert by_country.setdefault(key, partition) == partition
    assert "235 de 235 eventos entregados" in caplog.text
    assert "partición" in caplog.text and "offset" in caplog.text


def test_full_run_with_invalid_gives_the_expected_counts():
    fake = FakeKafka()
    assert run(["--with-invalid"], fake) == 0
    statuses = Counter(validate_event(value).status for _, _, value, _ in fake.sent)
    assert statuses == {VALID: 235, INVALID_SCHEMA: 3, INVALID_VALUES: 2}


def test_event_times_follow_the_sending_order():
    fake = FakeKafka()
    run(["--limit", "20"], fake)
    times = [validate_event(value).event["event_time"] for _, _, value, _ in fake.sent]
    assert len(times) == 20 and times == sorted(times)


# --- Fallas --------------------------------------------------------------------

def test_missing_topic_stops_before_sending(caplog, monkeypatch):
    monkeypatch.setattr(producer, "TOPIC_WAIT", 0)
    fake = FakeKafka(topics=())
    assert run(["--limit", "5"], fake) == 1
    assert fake.sent == [] and "no existe" in caplog.text


def test_waits_for_a_topic_that_is_still_being_created(caplog, monkeypatch):
    """Justo después de levantar Docker el topic puede tardar unos segundos en aparecer."""
    fake = FakeKafka(topics=())
    waits = []

    def fake_sleep(seconds):
        waits.append(seconds)
        if len(waits) == 2:
            fake.topics = (config.KAFKA_TOPIC,)

    monkeypatch.setattr(producer.time, "sleep", fake_sleep)
    with caplog.at_level("INFO", logger="producer"):
        assert run(["--limit", "3"], fake) == 0
    assert len(fake.sent) == 3 and len(waits) == 2 and "aún no existe" in caplog.text


def test_warns_when_the_topic_already_has_messages(caplog):
    fake = FakeKafka()
    with caplog.at_level("WARNING", logger="producer"):
        assert run(["--limit", "3"], fake, existing=240) == 0
    assert len(fake.sent) == 3  # solo avisa: el envío continúa
    assert "ya tiene 240 mensajes" in caplog.text and "reset_demo.py" in caplog.text


def test_no_warning_when_the_topic_is_empty(caplog):
    with caplog.at_level("WARNING", logger="producer"):
        assert run(["--limit", "3"], FakeKafka(), existing=0) == 0
    assert "ya tiene" not in caplog.text


def test_failure_counting_messages_does_not_stop_the_run(caplog):
    def broken(servers, topic):
        raise KafkaException(KafkaError(KafkaError._TRANSPORT))

    fake = FakeKafka()
    with caplog.at_level("WARNING", logger="producer"):
        assert run(["--limit", "3"], fake, counter=broken) == 0
    assert len(fake.sent) == 3 and "ya tiene" not in caplog.text


def test_missing_input_file_is_reported_without_traceback(caplog, tmp_path):
    assert run(["--input", str(tmp_path / "no.csv")], FakeKafka()) == 1
    assert "No existe el archivo" in caplog.text


def test_wrong_partition_count_only_warns(caplog):
    fake = FakeKafka(partitions=1)
    assert run(["--limit", "3"], fake) == 0
    assert len(fake.sent) == 3 and "particiones" in caplog.text


def test_delivery_failure_stops_the_run(caplog):
    fake = FakeKafka(fail_at=4)
    with caplog.at_level("INFO", logger="producer"):
        assert run(["--limit", "10"], fake) == 1
    assert len(fake.sent) == 3
    assert "no entregado (broker caído)" in caplog.text and "3 de 10 eventos entregados" in caplog.text


@pytest.mark.parametrize("argv", [["--delay", "-1"], ["--delay", "nan"], ["--limit", "0"], ["--limit", "-3"], ["--limit", "x"]])
def test_bad_arguments_are_rejected(argv):
    with pytest.raises(SystemExit) as exit_info:
        producer.parse_args(argv)
    assert exit_info.value.code == 2


def test_default_arguments():
    args = producer.parse_args([])
    assert (args.delay, args.limit, args.with_invalid, args.seed) == (0.3, None, False, config.RANDOM_STATE)
    assert args.input == config.STREAMING_FILE
    assert (args.bootstrap_servers, args.topic) == ("127.0.0.1:9092", "happiness-events")
