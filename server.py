import argparse
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, delay_ms=0, clock=None):
        self._lock = threading.Lock()
        self._values = {}   # counter_id -> int
        self._seen = {}     # idempotency_key -> (counter_id, resulting value)
        self._delay_ms = delay_ms
        self._clock = clock or LamportClock("replica")

    def Increment(self, request, context):
        desc = f"Increment(counter={request.counter_id}, delta={request.delta})"
        self._clock.receive(desc, request.lamport_time)
        if self._delay_ms:
            time.sleep(self._delay_ms / 1000.0)
        # Проверка ключа и изменение счётчика в ОДНОЙ критической секции
        with self._lock:
            if request.idempotency_key in self._seen:
                _, value = self._seen[request.idempotency_key]
                duplicate = True
                self._clock.local_event(
                    "DEDUP", f"counter={request.counter_id} stays {value}")
            else:
                value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = value
                self._seen[request.idempotency_key] = (request.counter_id, value)
                duplicate = False
                self._clock.local_event(
                    "APPLY", f"counter={request.counter_id} -> {value}")
        t = self._clock.send(f"IncrementReply(new_value={value})")
        return counter_pb2.IncrementReply(
            new_value=value, was_duplicate=duplicate, lamport_time=t)

    def Get(self, request, context):
        self._clock.receive(f"Get(counter={request.counter_id})", request.lamport_time)
        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)
        t = self._clock.send(f"GetReply(value={value}, found={found})")
        return counter_pb2.GetReply(value=value, found=found, lamport_time=t)


def start_server(port=0, delay_ms=0, name="replica", log_file=None, echo=False):
    """port=0 означает 'любой свободный порт' (удобно для тестов)."""
    clock = LamportClock(name, log_file, echo)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(delay_ms, clock), server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--name", default="replica")
    parser.add_argument("--log", default=None, help="файл для лога событий")
    args = parser.parse_args()

    server, port = start_server(args.port, args.delay_ms, args.name, args.log, echo=True)
    print(f"{args.name} listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)