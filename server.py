import argparse
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, delay_ms=0):
        self._lock = threading.Lock()
        self._values = {}   # counter_id -> int
        self._seen = {}     # idempotency_key -> (counter_id, resulting value)
        self._delay_ms = delay_ms  # искусственная задержка (понадобится для тестов таймаута)

    def Increment(self, request, context):
        if self._delay_ms:
            time.sleep(self._delay_ms / 1000.0)
        # Проверка ключа и изменение счётчика в ОДНОЙ критической секции
        with self._lock:
            if request.idempotency_key in self._seen:
                _, stored_value = self._seen[request.idempotency_key]
                return counter_pb2.IncrementReply(
                    new_value=stored_value, was_duplicate=True
                )
            new_value = self._values.get(request.counter_id, 0) + request.delta
            self._values[request.counter_id] = new_value
            self._seen[request.idempotency_key] = (request.counter_id, new_value)
            return counter_pb2.IncrementReply(
                new_value=new_value, was_duplicate=False
            )

    def Get(self, request, context):
        with self._lock:
            if request.counter_id in self._values:
                return counter_pb2.GetReply(
                    value=self._values[request.counter_id], found=True
                )
            return counter_pb2.GetReply(value=0, found=False)


def start_server(port=0, delay_ms=0):
    """Запускает сервер. port=0 означает 'любой свободный порт' (удобно для тестов)."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(delay_ms), server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()

    server, port = start_server(args.port, args.delay_ms)
    print(f"Replica listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)