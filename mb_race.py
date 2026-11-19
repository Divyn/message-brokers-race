"""Kafka, Redpanda, NATS JetStream, RabbitMQ and Iggy on the same 2 vCPU, 4 GB server.

Each broker runs alone in Docker with its quickstart settings. A second, identical server sends
200-byte messages at a fixed rate for 10 seconds with each broker's Python client, and waits for
the broker to confirm every message (Kafka and Redpanda: acks=all; NATS: JetStream publish ack;
RabbitMQ: publisher confirms on a durable queue, persistent messages; Iggy: send_messages returns).
Rates step up 1k, 2k, 5k, 10k, 20k, 50k messages a second, stopping at the first that fails.
A rate passes when at least 95% of its messages are confirmed and the 99th percentile confirm
time is at most 100 ms. Score: the highest rate that passed. 3 runs per broker; the median counts.
"""

import asyncio
import functools
import json
import statistics
import subprocess
import sys
import time

BROKERS = ["kafka", "redpanda", "nats", "rabbitmq", "iggy"]
RATES = [1_000, 2_000, 5_000, 10_000, 20_000, 50_000]
SECS, GRACE, REPEATS = 10, 5, 3
PAYLOAD = b"x" * 200
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]


def log(m: str) -> None:
    print("# " + m, file=sys.stderr, flush=True)


# ---------- one level, run in its own process ----------

def kafka_level(ip: str, rate: int) -> list[float]:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    AdminClient({"bootstrap.servers": f"{ip}:9092"}).create_topics([NewTopic("bench", 1, 1)])
    time.sleep(2)
    p = Producer({"bootstrap.servers": f"{ip}:9092", "acks": "all"})
    lat: list[float] = []

    def cb(t0, err, msg):
        if err is None:
            lat.append(time.perf_counter() - t0)

    total, sent, start = rate * SECS, 0, time.perf_counter()
    while sent < total:
        due = min(total, int((time.perf_counter() - start) * rate))
        while sent < due:
            try:
                p.produce("bench", PAYLOAD, on_delivery=functools.partial(cb, time.perf_counter()))
                sent += 1
            except BufferError:
                break
            p.poll(0)
        p.poll(0)
        if time.perf_counter() - start > SECS + GRACE:
            break
        time.sleep(0.0005)
    p.flush(GRACE)
    return lat


async def paced(rate: int, one) -> list[float]:
    """Fire one() rate times a second for SECS, each timed until its confirm comes back."""
    lat: list[float] = []

    async def timed():
        t0 = time.perf_counter()
        try:
            await one()
            lat.append(time.perf_counter() - t0)
        except Exception:
            pass

    tasks, total, sent, start = [], rate * SECS, 0, time.perf_counter()
    while sent < total and time.perf_counter() - start < SECS + GRACE:
        due = min(total, int((time.perf_counter() - start) * rate))
        while sent < due:
            tasks.append(asyncio.create_task(timed()))
            sent += 1
        await asyncio.sleep(0.0005)
    await asyncio.wait(tasks, timeout=GRACE) if tasks else None
    return list(lat)


async def nats_level(ip: str, rate: int) -> list[float]:
    import nats
    nc = await nats.connect(f"nats://{ip}:4222")
    js = nc.jetstream()
    try:
        await js.add_stream(name="bench", subjects=["bench"])
    except Exception:
        pass
    lat = await paced(rate, lambda: js.publish("bench", PAYLOAD))
    await nc.close()
    return lat


async def rabbitmq_level(ip: str, rate: int) -> list[float]:
    import aio_pika
    conn = await aio_pika.connect(f"amqp://bench:bench@{ip}/")
    ch = await conn.channel(publisher_confirms=True)
    await ch.declare_queue("bench", durable=True)
    ex = ch.default_exchange
    lat = await paced(rate, lambda: ex.publish(
        aio_pika.Message(PAYLOAD, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="bench"))
    await conn.close()
    return lat


async def iggy_level(ip: str, rate: int) -> list[float]:
    from apache_iggy import IggyClient, SendMessage
    client = IggyClient(f"{ip}:8090")
    await client.connect()
    await client.login_user("iggy", "iggy")
    for make in (lambda: client.create_stream(name="bench"),
                 lambda: client.create_topic(stream="bench", name="bench")):  # default: one partition
        try:
            await make()
        except Exception as exc:  # already exists from an earlier level
            log("iggy create: " + repr(exc)[:120])
    return await paced(rate, lambda: client.send_messages(  # apache-iggy 0.10 numbers partitions from 0
        stream="bench", topic="bench", partitioning=0, messages=[SendMessage(PAYLOAD.decode())]))


def run_level(broker: str, ip: str, rate: int) -> dict:
    if broker in ("kafka", "redpanda"):
        lat = kafka_level(ip, rate)
    else:
        lat = asyncio.run({"nats": nats_level, "rabbitmq": rabbitmq_level, "iggy": iggy_level}[broker](ip, rate))
    lat.sort()
    p99 = lat[int(len(lat) * 0.99) - 1] * 1000 if lat else float("inf")
    return {"confirmed": len(lat), "target": rate * SECS, "p99_ms": p99}


# ---------- the driver ----------

def level_state(r: dict) -> str:
    if r.get("error"):
        return "CLIENT_ERROR"
    if r["confirmed"] < 0.95 * r["target"]:
        return "UNDER_95_PERCENT_CONFIRMED"
    if r["p99_ms"] > 100:
        return "P99_OVER_100MS"
    return "PASS"


def level(broker: str, ip: str, rate: int) -> dict:
    res = subprocess.run([sys.executable, __file__, "level", broker, ip, str(rate)],
                         capture_output=True, text=True, timeout=120)
    try:
        return json.loads(res.stdout.strip().splitlines()[-1])
    except Exception:
        return {"error": (res.stderr or res.stdout)[-200:]}


def main() -> None:
    global BROKERS
    ip = sys.argv[1]
    BROKERS = sys.argv[2:] or BROKERS
    print("# " + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + f" rates={RATES} repeats={REPEATS}")
    scores = {b: [] for b in BROKERS}
    notes = {b: [] for b in BROKERS}
    for rep in range(REPEATS):
        for b in BROKERS[rep % len(BROKERS):] + BROKERS[:rep % len(BROKERS)]:
            up = subprocess.run(SSH + ["root@" + ip, f"bash /root/mb/up.sh {b} {ip}"],
                                capture_output=True, text=True, timeout=300)
            if "ready" not in up.stdout:
                notes[b].append("FAILED_TO_START " + up.stdout[-150:].replace(",", ";").replace("\n", " "))
                log(f"{b} failed to start")
                continue
            best, why = 0, "ALL_PASSED"
            for rate in RATES:
                r = level(b, ip, rate)
                st = level_state(r)
                log(f"rep {rep + 1} {b} {rate}/s -> {st} {r}")
                if st != "PASS":
                    why = f"{st} at {rate}"
                    if st == "CLIENT_ERROR":
                        why += " " + r["error"].replace(",", ";").replace("\n", " ")[-120:]
                    break
                best = rate
            scores[b].append(best)
            notes[b].append(why)
    print("broker,median_msgs_per_s,runs,stopped_by")
    for b in BROKERS:
        med = statistics.median(scores[b]) if scores[b] else ""
        print(f"{b},{med},{' '.join(map(str, scores[b]))},{' | '.join(notes[b])}")


if __name__ == "__main__":
    if sys.argv[1] == "level":
        print(json.dumps(run_level(sys.argv[2], sys.argv[3], int(sys.argv[4]))))
    else:
        main()
