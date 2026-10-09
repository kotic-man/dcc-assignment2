import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class QuorumError(Exception):
    """Запись не набрала большинство подтверждений."""


@dataclass
class CommitResult:
    new_value: int
    acks: int
    total: int


class CounterClient:
    def __init__(self, target="localhost:50051", timeout=2.0, backoffs=(0.2, 0.4, 0.8),
                 name="client", log_file=None, echo=False, clock=None):
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._timeout = timeout
        self._backoffs = backoffs
        self._clock = clock or LamportClock(name, log_file, echo)

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


class ReplicatedCounterClient:
    """Пишет на все реплики; запись успешна, если подтвердило большинство."""

    def __init__(self, targets, timeout=2.0, backoffs=(0.2, 0.4, 0.8),
                 name="client", log_file=None, echo=False):
        self._clock = LamportClock(name, log_file, echo)   # одни часы на весь клиент
        self._clients = [CounterClient(t, timeout, backoffs, clock=self._clock)
                         for t in targets]
        self._majority = len(targets) // 2 + 1
        self._pool = ThreadPoolExecutor(max_workers=len(targets))

    def incr(self, counter_id, delta, key=None):
        # ОДИН ключ на операцию: для всех реплик и всех повторных попыток
        key = key or str(uuid.uuid4())
        futures = [self._pool.submit(c.incr, counter_id, delta, key)
                   for c in self._clients]
        replies = []
        for f in futures:
            try:
                replies.append(f.result())
            except grpc.RpcError:
                pass                              # эта реплика не подтвердила
        if len(replies) < self._majority:
            raise QuorumError(
                f"quorum not reached ({len(replies)}/{len(self._clients)} acks)")
        return CommitResult(max(r.new_value for r in replies),
                            len(replies), len(self._clients))

    def get(self, counter_id):
        # Чтение с ОДНОЙ реплики: первая, которая ответила
        for c in self._clients:
            try:
                return c.get(counter_id)
            except grpc.RpcError:
                continue
        raise QuorumError("no replica reachable")

    def close(self):
        self._pool.shutdown(wait=False)
        for c in self._clients:
            c.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="localhost:50051")
    parser.add_argument("--targets", default=None,
                        help="реплики через запятую (режим кворума)")
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
    if args.targets:
        client = ReplicatedCounterClient(args.targets.split(","), name=args.name,
                                         log_file=args.log, echo=True)
    else:
        client = CounterClient(args.target, name=args.name,
                               log_file=args.log, echo=True)
    try:
        if args.cmd == "incr":
            r = client.incr(args.counter_id, args.by, args.key)
            if isinstance(r, CommitResult):
                print(f"OK committed value={r.new_value} (acks: {r.acks}/{r.total})")
            else:
                dup = "yes" if r.was_duplicate else "no"
                print(f"OK committed value={r.new_value} (duplicate: {dup})")
        else:
            r = client.get(args.counter_id)
            print(f"value={r.value}" if r.found else "not found")
    except QuorumError as e:
        print(f"FAILED: {e}")
        sys.exit(1)
    except grpc.RpcError as e:
        print(f"FAILED: {e.code().name}")
        sys.exit(1)