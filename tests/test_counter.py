import threading

import pytest

from client import CounterClient
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