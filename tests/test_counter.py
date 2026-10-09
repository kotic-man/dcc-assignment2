import threading
import time

import grpc
import pytest

from client import CounterClient, QuorumError, ReplicatedCounterClient
from server import start_server


class RunningServer:
    """Сервер на свободном порту + удобное создание клиентов."""

    def __init__(self, delay_ms=0):
        self.server, self.port = start_server(port=0, delay_ms=delay_ms)
        self._clients = []

    def new_client(self, **kwargs):
        client = CounterClient(f"localhost:{self.port}", **kwargs)
        self._clients.append(client)
        return client

    def stop(self):
        for c in self._clients:
            c.close()
        self.server.stop(0)


@pytest.fixture
def running_server():
    rs = RunningServer()
    yield rs
    rs.stop()


class ReplicaGroup:
    """Три независимые реплики на свободных портах."""

    def __init__(self, n=3):
        self.servers, self.ports = [], []
        self._stopped = set()
        self._clients = []
        for i in range(n):
            server, port = start_server(port=0, name=f"replica-{'ABCDE'[i]}")
            self.servers.append(server)
            self.ports.append(port)

    @property
    def targets(self):
        return [f"localhost:{p}" for p in self.ports]

    def new_client(self):
        # короткие паузы между повторами, чтобы тесты шли быстро
        c = ReplicatedCounterClient(self.targets, timeout=1.0, backoffs=(0.05, 0.05))
        self._clients.append(c)
        return c

    def direct_client(self, i):
        """Клиент, который говорит только с одной конкретной репликой."""
        c = CounterClient(self.targets[i], timeout=1.0)
        self._clients.append(c)
        return c

    def stop_replica(self, i):
        if i not in self._stopped:
            self.servers[i].stop(0)
            self._stopped.add(i)

    def stop(self):
        for c in self._clients:
            c.close()
        for i in range(len(self.servers)):
            self.stop_replica(i)


@pytest.fixture
def replicas():
    group = ReplicaGroup(3)
    yield group
    group.stop()


def test_increment_applies_delta(running_server):
    client = running_server.new_client()                 # arrange
    reply = client.incr("x", 7)                          # act
    assert reply.new_value == 7                          # assert
    assert reply.was_duplicate is False
    assert client.get("x").value == 7


def test_duplicate_key_not_reapplied(running_server):
    client = running_server.new_client()
    r1 = client.incr("x", 5, key="k-1")
    r2 = client.incr("x", 5, key="k-1")                  # retry with same key
    assert r1.new_value == 5
    assert r2.new_value == 5 and r2.was_duplicate        # not 10
    assert client.get("x").value == 5


def test_get_missing_counter(running_server):
    client = running_server.new_client()
    reply = client.get("no-such-counter")
    assert reply.found is False


def test_concurrent_increments_exact(running_server):
    PER_THREAD = 1000
    errors = []

    def worker():
        try:
            c = running_server.new_client()              # свой клиент на поток
            for _ in range(PER_THREAD):
                c.incr("shared", 1)                      # новый ключ на каждый вызов
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert running_server.new_client().get("shared").value == 2 * PER_THREAD


def test_retry_after_timeout_is_safe():
    # arrange: сервер отвечает медленнее, чем дедлайн клиента
    rs = RunningServer(delay_ms=300)
    try:
        impatient = rs.new_client(timeout=0.1, backoffs=())    # без внутренних повторов
        # act 1: первая попытка заканчивается таймаутом (клиент не знает, применился ли запрос)
        with pytest.raises(grpc.RpcError) as exc:
            impatient.incr("x", 1, key="k-timeout")
        assert exc.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        time.sleep(0.6)    # сервер дорабатывает первый запрос, хотя клиент ушёл
        # act 2: повтор с ТЕМ ЖЕ ключом и нормальным дедлайном
        patient = rs.new_client(timeout=3.0)
        retry = patient.incr("x", 1, key="k-timeout")
        # assert: счётчик сдвинулся ровно один раз
        assert retry.was_duplicate is True
        assert retry.new_value == 1
        assert patient.get("x").value == 1
    finally:
        rs.stop()


def test_majority_commit_two_acks(replicas):
    client = replicas.new_client()                       # arrange
    replicas.stop_replica(2)                             # одна реплика упала
    result = client.incr("x", 1)                         # act
    assert result.acks == 2                              # assert: 2 из 3 = большинство
    assert result.total == 3
    assert result.new_value == 1


def test_no_commit_below_majority(replicas):
    client = replicas.new_client()
    replicas.stop_replica(1)                             # упали две из трёх
    replicas.stop_replica(2)
    with pytest.raises(QuorumError):                     # клиент честно сообщает об ошибке
        client.incr("x", 1)
    # выжившая реплика запись всё же применила (частично применённая запись)
    assert replicas.direct_client(0).get("x").value == 1


def test_replicas_converge(replicas):
    client = replicas.new_client()
    for i in range(10):                                  # 10 записей при трёх живых репликах
        client.incr("a" if i % 2 == 0 else "b", 1)
    replicas.stop_replica(2)                             # одна реплика падает
    for i in range(10, 15):                              # ещё 5 записей при двух живых
        client.incr("a" if i % 2 == 0 else "b", 1)

    # живые реплики 0 и 1 идентичны: a получил 8 записей, b получил 7
    for idx in (0, 1):
        direct = replicas.direct_client(idx)
        assert direct.get("a").value == 8
        assert direct.get("b").value == 7