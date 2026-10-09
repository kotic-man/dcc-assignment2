import argparse
import os
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

FAULTS = ("none", "crash-after-recv", "slow-first")


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, delay_ms=0, clock=None, fault="none"):
        self._lock = threading.Lock()
        self._values = {}   # counter_id -> int
        self._seen = {}     # idempotency_key -> (counter_id, resulting value)
        self._delay_ms = delay_ms
        self._clock = clock or LamportClock("replica")
        self._fault = fault
        self._fault_lock = threading.Lock()
        self._fault_used = False

    def _first_increment(self):
        """True только для самого первого Increment, который получила реплика."""
        with self._fault_lock:
            first = not self._fault_used
            self._fault_used = True
            return first

    def Increment(self, request, context):
        desc = f"Increment(counter={request.counter_id}, delta={request.delta})"
        self._clock.receive(desc, request.lamport_time)

        delay_ms = self._delay_ms
        if self._fault == "crash-after-recv" and self._first_increment():
            os._exit(1)   # реплика умирает, пока запрос в полёте
        if self._fault == "slow-first":
            delay_ms = delay_ms if self._first_increment() else 0
        if delay_ms:
            time.sleep(delay_ms / 1000.0)

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


def start_server(port=0, delay_ms=0, name="replica", log_file=None, echo=False,
                 fault="none"):
    """port=0 означает 'любой свободный порт' (удобно для тестов)."""
    clock = LamportClock(name, log_file, echo)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(delay_ms, clock, fault), server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--fault", choices=FAULTS, default="none")
    parser.add_argument("--name", default="replica")
    parser.add_argument("--log", default=None, help="файл для лога событий")
    parser.add_argument("--quiet", action="store_true",
                        help="не печатать события в консоль")
    args = parser.parse_args()

    server, port = start_server(args.port, args.delay_ms, args.name, args.log,
                                echo=not args.quiet, fault=args.fault)
    print(f"{args.name} listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)