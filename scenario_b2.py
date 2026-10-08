import os
import threading
import time

from client import CounterClient

TARGET = "localhost:50051"


def client_1():
    c = CounterClient(TARGET, name="client-1", log_file="logs/client-1.log")
    c.incr("x", 1)
    c.incr("x", 1)
    c.close()


def client_2():
    c = CounterClient(TARGET, name="client-2", log_file="logs/client-2.log")
    c.incr("y", 1)
    c.incr("y", 1)
    c.close()


def client_3():
    c = CounterClient(TARGET, name="client-3", log_file="logs/client-3.log")
    time.sleep(0.05)
    c.get("x")
    time.sleep(0.05)
    c.get("y")
    c.close()


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    threads = [threading.Thread(target=f) for f in (client_1, client_2, client_3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("scenario finished")