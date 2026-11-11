"""Pruebas del script de reinicio con un Kafka falso (el flujo real se verifica contra Docker)."""
import importlib.util
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from happiness_etl import config

_spec = importlib.util.spec_from_file_location("reset_demo", config.ROOT_DIR / "scripts" / "reset_demo.py")
reset_demo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reset_demo)


class Future:
    def __init__(self, value=None, error_code=None):
        self._value, self._error_code = value, error_code

    def result(self):
        if self._error_code is not None:
            raise KafkaException(KafkaError(self._error_code))
        return self._value


class FakeAdmin:
    """Guarda topics y grupos en memoria. El borrado de un topic tarda 'lag' intentos en verse."""

    def __init__(self, topics=None, groups=None, lag=0):
        self.topics, self.groups, self.lag = dict(topics or {}), dict(groups or {}), lag
        self._deleting = 0
        self.calls = []

    def describe_consumer_groups(self, groups, request_timeout):
        return {g: Future(SimpleNamespace(members=["m"] * self.groups.get(g, 0))) for g in groups}

    def delete_consumer_groups(self, groups, request_timeout):
        self.calls.append("delete_group")
        return {g: Future() if self.groups.pop(g, None) is not None else Future(error_code=KafkaError.GROUP_ID_NOT_FOUND)
                for g in groups}

    def delete_topics(self, topics, operation_timeout):
        self.calls.append("delete_topic")
        out = {}
        for topic in topics:
            if topic in self.topics:
                del self.topics[topic]
                self._deleting = self.lag
                out[topic] = Future()
            else:
                out[topic] = Future(error_code=KafkaError.UNKNOWN_TOPIC_OR_PART)
        return out

    def create_topics(self, new_topics, operation_timeout):
        self.calls.append("create_topic")
        out = {}
        for new in new_topics:
            if self._deleting > 0:
                self._deleting -= 1
                out[new.topic] = Future(error_code=KafkaError.TOPIC_ALREADY_EXISTS)
            else:
                self.topics[new.topic] = new.num_partitions
                out[new.topic] = Future()
        return out

    def list_topics(self, timeout):
        return SimpleNamespace(topics={
            name: SimpleNamespace(error=None, partitions=dict.fromkeys(range(count))) for name, count in self.topics.items()
        })


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(reset_demo.time, "sleep", lambda seconds: None)


def test_recreate_existing_topic_retries_until_the_deletion_is_visible():
    admin = FakeAdmin(topics={"happiness-events": 3}, lag=2)
    assert reset_demo.recreate_topic(admin, "happiness-events", 3) is True
    assert admin.topics == {"happiness-events": 3}
    assert admin.calls == ["delete_topic", "create_topic", "create_topic", "create_topic"]


def test_recreate_fixes_a_topic_with_the_wrong_partition_count():
    admin = FakeAdmin(topics={"happiness-events": 1})
    reset_demo.recreate_topic(admin, "happiness-events", 3)
    assert admin.topics == {"happiness-events": 3}


def test_recreate_creates_the_topic_when_it_did_not_exist():
    admin = FakeAdmin()
    assert reset_demo.recreate_topic(admin, "happiness-events", 3) is False
    assert admin.topics == {"happiness-events": 3}


def test_unexpected_kafka_errors_are_not_hidden():
    admin = FakeAdmin()
    admin.delete_topics = lambda topics, operation_timeout: {t: Future(error_code=KafkaError._TIMED_OUT) for t in topics}
    with pytest.raises(KafkaException):
        reset_demo.recreate_topic(admin, "happiness-events", 3)


def test_delete_group_reports_whether_it_existed():
    admin = FakeAdmin(groups={"happiness-ml-team": 0})
    assert reset_demo.delete_group(admin, "happiness-ml-team") is True
    assert reset_demo.delete_group(admin, "happiness-ml-team") is False


def test_active_members_counts_connected_consumers():
    admin = FakeAdmin(groups={"happiness-ml-team": 2})
    assert reset_demo.active_members(admin, "happiness-ml-team") == 2
    assert reset_demo.active_members(admin, "otro-grupo") == 0
