#!/usr/bin/env python3
# send_hsutest_events.py
"""
Async JSON producer for Kafka topic 'hsutest' using aiokafka.

Schema:
{
  "user":   str,
  "event":  str,
  "amount": float,
  "ts":     int   # unix epoch millis
}

Run:
  pip install aiokafka
  python send_hsutest_events.py --bootstrap localhost:9092 --topic hsutest --rps 10
"""

import asyncio
import argparse
import json
import random
import signal
import string
import time
from typing import Dict, Any

from aiokafka import AIOKafkaProducer


def now_millis() -> int:
    return int(time.time() * 1000)


def rand_user(n: int = 6) -> str:
    # e.g., "u_ab12cd"
    letters = string.ascii_lowercase + string.digits
    return "u_" + "".join(random.choice(letters) for _ in range(n))


def make_event() -> Dict[str, Any]:
    return {
        "user": rand_user(),
        "event": random.choice(["click", "view", "purchase", "add_to_cart", "login"]),
        "amount": round(max(0.0, random.gauss(10.0, 5.0)), 2),  # skewed positive
        "ts": now_millis(),
    }


async def produce_forever(bootstrap: str, topic: str, rps: int, key_by_user: bool):
    """
    Produce ~rps messages per second forever (Ctrl+C to stop).
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        # serialize Python dict -> JSON bytes
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=(lambda k: k.encode("utf-8")) if key_by_user else None,
        linger_ms=5,  # small batching; tweak as needed
        acks="all",  # safest
        enable_idempotence=True,
    )

    await producer.start()
    print(f"[producer] started, topic='{topic}', bootstrap='{bootstrap}', rps={rps}")
    try:
        # token-based throttling
        delay = 1.0 / max(1, rps)
        while True:
            evt = make_event()
            key = evt["user"] if key_by_user else None

            # send and wait for broker ack (awaitable)
            md = await producer.send_and_wait(topic, value=evt, key=key)

            print(
                f"sent: partition={md.partition} offset={md.offset} key={key} value={evt}"
            )
            await asyncio.sleep(delay)
    finally:
        print("[producer] stopping…")
        await producer.stop()
        print("[producer] stopped.")


def main():
    ap = argparse.ArgumentParser(description="aiokafka JSON producer for topic hsutest")
    ap.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap servers (host:port[,host:port…])",
    )
    ap.add_argument("--topic", default="hsudemo", help="Kafka topic name")
    ap.add_argument("--rps", type=int, default=20, help="messages per second")
    ap.add_argument(
        "--seed", type=int, default=None, help="random seed for reproducibility"
    )
    ap.add_argument(
        "--key-by-user",
        action="store_true",
        help="use 'user' field as Kafka message key (enables key-based partitioning)",
    )
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    loop = asyncio.get_event_loop()

    # graceful Ctrl+C
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, loop.stop)
        except NotImplementedError:
            # Windows may not support signal handlers in event loop
            pass

    task = loop.create_task(
        produce_forever(args.bootstrap, args.topic, args.rps, args.key_by_user)
    )
    try:
        loop.run_forever()
    finally:
        if not task.done():
            task.cancel()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
        loop.close()


if __name__ == "__main__":
    main()
