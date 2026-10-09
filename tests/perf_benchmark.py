import math
import os
import socket
import statistics
import subprocess
import sys
import threading
import time

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from client import CounterClient, ReplicatedCounterClient  # noqa: E402

REQUESTS = 2000   # на каждую конфигурацию
WARMUP = 50       # прогрев каждого клиента, в замер не входит


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


class Replica:
    """Реплика как отдельный процесс (без консольного вывода)."""

    def __init__(self, name):
        self.port = free_port()
        self.target = f"localhost:{self.port}"
        self.proc = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(self.port),
             "--name", name, "--quiet"],
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


def p95(sorted_values):
    """Значение с рангом ceil(0.95 * n) (нумерация с 1), целочисленно, без ошибок float."""
    n = len(sorted_values)
    rank = -(-95 * n // 100)
    return sorted_values[min(rank, n) - 1]


def run_config(make_client, n_clients, total):
    per_client = total // n_clients
    results = [[] for _ in range(n_clients)]
    errors = []
    barrier = threading.Barrier(n_clients)

    def worker(i):
        client = make_client()
        try:
            for _ in range(WARMUP):
                client.incr(f"warm-{i}", 1)
            barrier.wait(timeout=120)             # все стартуют одновременно
            for _ in range(per_client):
                t0 = time.perf_counter()
                client.incr(f"bench-{i}", 1)
                results[i].append((time.perf_counter() - t0) * 1000.0)
        except Exception as e:
            errors.append(e)
            barrier.abort()
        finally:
            client.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(f"benchmark failed: {errors[0]!r}")
    return sorted(x for lat in results for x in lat)


def main():
    replicas = [Replica(f"bench-{n}") for n in "ABC"]
    try:
        single = replicas[0].target
        targets = [r.target for r in replicas]
        configs = [
            ("Single replica, 1 client", lambda: CounterClient(single, timeout=5.0), 1),
            ("Single replica, 16 clients", lambda: CounterClient(single, timeout=5.0), 16),
            ("Quorum (3 replicas), 1 client",
             lambda: ReplicatedCounterClient(targets, timeout=5.0), 1),
            ("Quorum (3 replicas), 16 clients",
             lambda: ReplicatedCounterClient(targets, timeout=5.0), 16),
        ]
        rows = []
        for name, make_client, n_clients in configs:
            print(f"running: {name} ...", file=sys.stderr, flush=True)
            lat = run_config(make_client, n_clients, REQUESTS)
            rows.append((name, statistics.median(lat), p95(lat), len(lat)))
    finally:
        for r in replicas:
            r.stop()

    print("| Configuration | Median latency (ms) | p95 latency (ms) | Requests |")
    print("| --- | --- | --- | --- |")
    for name, med, p, n in rows:
        print(f"| {name} | {med:.2f} | {p:.2f} | {n} |")


if __name__ == "__main__":
    main()