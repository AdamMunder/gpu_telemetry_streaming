"""One command to start the whole pipeline, and one Ctrl+C to stop it in the right order.

    python run_pipeline.py

START order (each layer listens before data reaches it):
    docker (kafka + spark)  ->  bronze  ->  silver  ->  gold  ->  dashboard  ->  producer
STOP order (Ctrl+C), the same direction the data flows:
    producer  ->  wait so the last events reach gold  ->  gold, silver, bronze  ->  dashboard

Each Spark job runs in its own short-lived container (pipe-bronze, pipe-silver, pipe-gold),
so it can be stopped cleanly with "docker stop". Its checkpoint lets it continue next time.
"""
import os
import signal
import subprocess
import sys
import threading
import time

DRAIN_SECONDS = 25        # after stopping the producer, time for the last events to reach gold
READY_TIMEOUT = 300       # max seconds to wait for a Spark job to start streaming

SPARK_JOBS = [            # (label, script, line that means "ready")
    ("bronze", "pipeline/01_bronze.py", "streaming from Kafka"),
    ("silver", "pipeline/02_silver.py", "streaming from bronze"),
    ("gold", "pipeline/03_gold.py", "streaming from silver"),
]

IS_WINDOWS = os.name == "nt"
# Children get their own process group, so YOUR Ctrl+C reaches only this launcher,
# which then stops them one by one in the right order.
NEW_GROUP = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if IS_WINDOWS \
    else {"start_new_session": True}

processes = {}            # label -> running process
ready_events = {}         # label -> set when the "ready" line appears


def pipe_output(label, proc, ready_text):
    """Print a child's output with a [label] prefix, and notice when it is ready."""
    for raw in proc.stdout:
        line = raw.rstrip()
        print(f"[{label:9}] {line}", flush=True)
        if ready_text and ready_text in line:
            ready_events[label].set()


def start(label, cmd, ready_text=None):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", errors="replace", bufsize=1, **NEW_GROUP)
    processes[label] = proc
    ready_events[label] = threading.Event()
    threading.Thread(target=pipe_output, args=(label, proc, ready_text), daemon=True).start()
    return proc


def run_quiet(cmd):
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_until_ready(label):
    print(f">>> waiting for {label} to start streaming...", flush=True)
    deadline = time.time() + READY_TIMEOUT
    while time.time() < deadline:
        if ready_events[label].wait(1):
            print(f">>> {label} is streaming", flush=True)
            return
        if processes[label].poll() is not None:
            raise RuntimeError(f"{label} exited before it was ready (see its output above)")
    raise RuntimeError(f"{label} was not ready after {READY_TIMEOUT} seconds")


def stop_producer():
    proc = processes.get("producer")
    if proc is None or proc.poll() is not None:
        return
    print(">>> stopping producer", flush=True)
    if IS_WINDOWS:
        proc.send_signal(signal.CTRL_BREAK_EVENT)   # producer turns this into a clean stop
    else:
        proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()


def stop_everything():
    stop_producer()
    if any(label in processes for label, _, _ in SPARK_JOBS):
        print(f">>> waiting {DRAIN_SECONDS}s so the last events reach gold...", flush=True)
        time.sleep(DRAIN_SECONDS)
    for label, _, _ in reversed(SPARK_JOBS):        # gold, silver, bronze
        if label in processes:
            print(f">>> stopping {label}", flush=True)
            run_quiet(["docker", "stop", "-t", "30", f"pipe-{label}"])
    dash = processes.get("dashboard")
    if dash is not None and dash.poll() is None:
        print(">>> stopping dashboard", flush=True)
        dash.terminate()
    print(">>> pipeline stopped. Kafka and Spark containers are still up "
          "(use 'docker compose stop' to stop them too).", flush=True)


def main():
    print(">>> starting Kafka and Spark containers", flush=True)
    subprocess.run(["docker", "compose", "up", "-d"], check=True)
    for label, _, _ in SPARK_JOBS:                  # clean up leftovers from a crashed run
        run_quiet(["docker", "rm", "-f", f"pipe-{label}"])

    try:
        for label, script, ready_text in SPARK_JOBS:
            start(label, ["docker", "compose", "run", "--rm", "-T", "--name", f"pipe-{label}",
                          "-e", "PYTHONUNBUFFERED=1",  # print lines immediately, not in chunks
                          "spark", "/opt/spark/bin/spark-submit", script], ready_text)
            wait_until_ready(label)

        start("dashboard", [sys.executable, "-m", "streamlit", "run", "dashboard/app.py"])
        time.sleep(3)
        start("producer", [sys.executable, "producer/produce.py"])

        print("\n>>> PIPELINE IS LIVE. Dashboard: http://localhost:8501   Press Ctrl+C to stop.\n",
              flush=True)
        while True:                                  # watch for a job that dies on its own
            for label, proc in processes.items():
                if proc.poll() is not None:
                    raise RuntimeError(f"{label} stopped unexpectedly (see its output above)")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n>>> Ctrl+C received, stopping in order...", flush=True)
    except RuntimeError as err:
        print(f"\n>>> PROBLEM: {err}. Stopping everything...", flush=True)
    finally:
        stop_everything()


if __name__ == "__main__":
    main()