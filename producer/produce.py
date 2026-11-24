"""Simulated GPU telemetry producer.

Pretends to be 4 GPUs. Every second each GPU sends one JSON event to the
Kafka topic 'gpu-telemetry'. The message KEY is the gpu_id, so all events of one GPU always
land in the same partition (and stay in order).

Some events are broken on purpose (null temperature, impossible temperature, duplicate,
unknown GPU) so the silver layer later has something to clean.

Usage:
    python producer/produce.py        # runs forever, press Ctrl+C to stop
"""
import json
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "gpu-telemetry"
BOOTSTRAP_SERVERS = "localhost:9092"  # the Kafka container, seen from Windows
INTERVAL_SECONDS = 1.0                # wait between rounds of events
BAD_RATE = 0.08                       # 8% of events get broken on purpose

# this is the info of our virtual gpus they match the data in the csv that will be used later for BI
GPU_PROFILES = {
    "GPU-01": {"node_id": "node-1", "rated_power_w": 400, "base_temp": 68.0},
    "GPU-02": {"node_id": "node-1", "rated_power_w": 400, "base_temp": 72.0},
    "GPU-03": {"node_id": "node-2", "rated_power_w": 700, "base_temp": 76.0},
    "GPU-04": {"node_id": "node-2", "rated_power_w": 700, "base_temp": 80.0},
}


# this method defines the flags related to each temperature range: <80 OK, 80-90 WARNING, >90 DANGER.
def status_for(temp_c):
    if temp_c > 90:
        return "DANGER"
    if temp_c >= 80:
        return "WARNING"
    return "OK"


# we create a class for the simulator object
class GpuSimulator:
    """the GpuSimulator have this attributes:
     1) random number generator
     2) temperature stored for each gpu (dictionary)
     3) utilization percentage for each gpu (dictionary)"""

    def __init__(self, rng):
        self.rng = rng  # rng means random number generator
        self.temp = {}
        for g, p in GPU_PROFILES.items():  # go through each GPU profile, g is the gpu key and p is its profile
            self.temp[g] = p["base_temp"]  # store that GPU base temperature under its name
        self.utilization_percentage = {g: 80.0 for g in GPU_PROFILES}  # initialize utilization pct to 80%

    def next_event(self, gpu_id):
        profile = GPU_PROFILES[gpu_id]
        """
        pull is the small step we set the gpu to take toward the
        base temp because if we jump in large step back to the good temp it will harm the GPU
        """
        pull = (profile["base_temp"] - self.temp[gpu_id]) * 0.1
        self.temp[gpu_id] += self.rng.uniform(-1.5, 1.5) + pull  # update the temperature of the GPU
        if self.rng.random() < 0.02:  # occasional heat spike happens 2% of the times
            self.temp[gpu_id] += self.rng.uniform(8, 16)
        self.temp[gpu_id] = max(45.0, min(105.0, self.temp[gpu_id]))  # the range of possible temp is [45,105]
        # utilization: random walk between 30 and 100
        self.utilization_percentage[gpu_id] = max(
            30.0, min(100.0, self.utilization_percentage[gpu_id] + self.rng.uniform(-5, 5)))

        temp = round(self.temp[gpu_id], 1)
        util = round(self.utilization_percentage[gpu_id])
        # store the event in dictionary and return it, it's converted to JSON before it's sent to our kafka topic
        return {
            "gpu_id": gpu_id,
            "node_id": profile["node_id"],
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temp_c": temp,
            "power_w": round(profile["rated_power_w"] * (0.35 + 0.65 * util / 100)),
            "util_pct": util,
            "status": status_for(temp),
        }


def maybe_break(event, rng, bad_rate):
    """this function gets an event and bad_rate% of the times it ruins the event with 1 of four possible "noises"
     such as null value or duplicate event and it returns either 1 valid event, or 1 ruined event or 2 events
     (original + duplicate)
     THE GOAL OF THIS FUNCTION: is to mimic the real noise in the data that the GPU produces in real world
    """
    if rng.random() >= bad_rate:
        return [event]
    kind = rng.choice(["null_temp", "impossible_temp", "duplicate", "unknown_gpu"])
    if kind == "null_temp":
        return [dict(event, temp_c=None, status=None)]
    if kind == "impossible_temp":
        return [dict(event, temp_c=400.0)]
    if kind == "duplicate":
        return [event, dict(event)]  # exactly the same event sent twice
    return [dict(event, gpu_id="GPU-67")]  # a GPU that is not in the inventory


def on_delivery(err, msg):
    """Kafka calls this when a message is confirmed (or failed)."""
    if err is not None:
        print(f"DELIVERY FAILED: {err}")
    else:
        print(f"{msg.key().decode():8} -> partition {msg.partition()} offset {msg.offset()}")


def main():
    rng = random.Random()  # random number generator
    sim = GpuSimulator(rng)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    try:
        while True:  # run until Ctrl+C
            for gpu_id in GPU_PROFILES:
                for event in maybe_break(sim.next_event(gpu_id), rng, BAD_RATE):
                    producer.produce(TOPIC, key=event["gpu_id"], value=json.dumps(event),
                                     on_delivery=on_delivery)  # key = gpu_id decides the partition
                    producer.poll(0)  # lets Kafka's client report deliveries
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush(10)  # make sure everything buffered is really sent


if __name__ == "__main__":
    # On Windows, run_pipeline.py stops the producer with Ctrl+Break.
    # This line turns Ctrl+Break into the same clean stop as Ctrl+C (so flush still runs).
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, signal.default_int_handler)
    main()