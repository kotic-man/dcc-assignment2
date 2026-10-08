import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    def __init__(self, target="localhost:50051", timeout=2.0, backoffs=(0.2, 0.4, 0.8),
                 name="client", log_file=None, echo=False):
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._timeout = timeout
        self._backoffs = backoffs
        self._clock = LamportClock(name, log_file, echo)

    def incr(self, counter_id, delta, key=None):
        # Ключ создаётся ОДИН раз на операцию, а не на каждую попытку
        key = key or str(uuid.uuid4())
        desc = f"Increment(counter={counter_id}, delta={delta})"
        for attempt in range(len(self._backoffs) + 1):
            t = self._clock.send(desc)          # каждая попытка = событие SEND
            request = counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta,
                idempotency_key=key, lamport_time=t)
            try:
                reply = self._stub.Increment(request, timeout=self._timeout)
                self._clock.receive(
                    f"IncrementReply(new_value={reply.new_value})", reply.lamport_time)
                return reply
            except grpc.RpcError as e:
                last_attempt = attempt == len(self._backoffs)
                if e.code() not in RETRYABLE or last_attempt:
                    raise
                time.sleep(self._backoffs[attempt])  # 0.2 -> 0.4 -> 0.8 с

    def get(self, counter_id):
        t = self._clock.send(f"Get(counter={counter_id})")
        request = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=t)
        reply = self._stub.Get(request, timeout=self._timeout)
        self._clock.receive(
            f"GetReply(value={reply.value}, found={reply.found})", reply.lamport_time)
        return reply

    def close(self):
        self._channel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="localhost:50051")
    parser.add_argument("--name", default="client")
    parser.add_argument("--log", default=None, help="файл для лога событий")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    client = CounterClient(args.target, name=args.name, log_file=args.log, echo=True)
    try:
        if args.cmd == "incr":
            r = client.incr(args.counter_id, args.by, args.key)
            dup = "yes" if r.was_duplicate else "no"
            print(f"OK committed value={r.new_value} (duplicate: {dup})")
        else:
            r = client.get(args.counter_id)
            print(f"value={r.value}" if r.found else "not found")
    except grpc.RpcError as e:
        print(f"FAILED: {e.code().name}")
        sys.exit(1)