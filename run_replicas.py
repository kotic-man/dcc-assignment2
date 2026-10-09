import subprocess
import sys

NAMES = "ABCDEFGH"

if __name__ == "__main__":
    ports = sys.argv[1:] or ["50051", "50052", "50053"]
    procs = []
    for i, port in enumerate(ports):
        procs.append(subprocess.Popen(
            [sys.executable, "server.py", "--port", port, "--name", f"replica-{NAMES[i]}"]))
    print(f"Started {len(procs)} replicas on ports {', '.join(ports)}. Ctrl+C to stop.",
          flush=True)
    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()