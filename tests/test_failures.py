import os
import socket
import subprocess
import sys
import time

import grpc
import pytest

from client import CounterClient, ReplicatedCounterClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(ROOT, "logs", "failures")


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def log_path(name):
    """Путь к логу сценария; старый лог удаляется, чтобы не накапливались строки."""
    os.makedirs(LOG_DIR, exist_ok=True)
    path = os.path.join(LOG_DIR, f"{name}.log")
    if os.path.exists(path):
        os.remove(path)
    return path


def read_value(target, counter_id):
    client = CounterClient(target, timeout=2.0)
    try:
        return client.get(counter_id).value
    finally:
        client.close()


class ReplicaProcess:
    """Реплика как отдельный процесс."""

    def __init__(self, name, fault="none", delay_ms=0):
        self.port = free_port()
        self.target = f"localhost:{self.port}"
        self.proc = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(self.port), "--name", name,
             "--fault", fault, "--delay-ms", str(delay_ms), "--log", log_path(name)],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        channel = grpc.insecure_channel(self.target)
        try:
            grpc.channel_ready_future(channel).result(timeout=15)
        finally:
            channel.close()

    def stop(self):
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait()


@pytest.fixture
def launch():
    started = []

    def _launch(name, fault="none", delay_ms=0):
        replica = ReplicaProcess(name, fault, delay_ms)
        started.append(replica)
        return replica

    yield _launch
    for r in started:
        r.stop()


def test_replica_crash_mid_request_still_commits(launch):
    # arrange: replica C умрёт, получив первый запрос
    a = launch("crash-A")
    b = launch("crash-B")
    c = launch("crash-C", fault="crash-after-recv")
    client = ReplicatedCounterClient(
        [a.target, b.target, c.target], timeout=1.0, backoffs=(0.05, 0.05),
        name="crash-client", log_file=log_path("crash-client"))
    try:
        # act: исключение наружу вылететь не должно
        result = client.incr("x", 1)
    finally:
        client.close()
    # assert: коммит по двум оставшимся подтверждениям
    assert result.acks == 2 and result.total == 3
    assert result.new_value == 1
    assert c.proc.wait(timeout=5) == 1            # процесс реально умер
    assert read_value(a.target, "x") == 1         # выжившие реплики согласованы
    assert read_value(b.target, "x") == 1


def test_duplicate_request_moves_value_once(launch):
    # arrange
    replicas = [launch(f"dup-{n}") for n in "ABC"]
    client = ReplicatedCounterClient(
        [r.target for r in replicas], timeout=1.0, name="dup-client",
        log_file=log_path("dup-client"))
    try:
        first = client.incr("x", 5, key="dup-1")
        second = client.incr("x", 5, key="dup-1")   # тот же ключ ещё раз
    finally:
        client.close()
    assert first.new_value == 5
    assert second.new_value == 5                    # не 10
    # прямое повторное обращение к каждой реплике: был дубликат
    for r in replicas:
        direct = CounterClient(r.target, timeout=2.0)
        try:
            reply = direct.incr("x", 5, key="dup-1")
            assert reply.was_duplicate is True
            assert reply.new_value == 5
            assert direct.get("x").value == 5
        finally:
            direct.close()


def test_induced_timeout_retry_moves_exactly_once(launch):
    # arrange: первый запрос задерживается на 1.5 с, дедлайн клиента 0.5 с
    slow = launch("timeout-A", fault="slow-first", delay_ms=1500)
    client = CounterClient(slow.target, timeout=0.5, backoffs=(0.1, 0.2, 0.4),
                           name="timeout-client", log_file=log_path("timeout-client"))
    try:
        # act: 1-я попытка получает DEADLINE_EXCEEDED, повтор с тем же ключом проходит
        reply = client.incr("x", 1)
        assert reply.new_value == 1
        assert reply.was_duplicate is False         # повтор применился первым
        time.sleep(1.5)                             # исходный задержанный запрос доработает
        # assert: он распознан как дубликат, счётчик сдвинулся ровно один раз
        assert client.get("x").value == 1
    finally:
        client.close()