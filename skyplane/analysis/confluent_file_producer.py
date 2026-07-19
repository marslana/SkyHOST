#!/usr/bin/env python3
import argparse
import os
import time
import uuid
from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser(description="High-throughput confluent_kafka producer that reads a file line-by-line")
    ap.add_argument("--brokers", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"])
    ap.add_argument("--linger-ms", type=int, default=50)
    ap.add_argument("--max-in-flight", type=int, default=5)
    ap.add_argument("--no-headers", action="store_true")
    ap.add_argument("--rate", type=int, default=0, help="msgs/s; 0 = unthrottled")
    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.brokers,
        "acks": args.acks,
        "compression.type": "none",
        "linger.ms": args.linger_ms,
        "batch.num.messages": 100000,
        "queue.buffering.max.kbytes": 262144,
        "queue.buffering.max.messages": 1000000,
        "message.max.bytes": 8 * 1024 * 1024,
        "max.in.flight.requests.per.connection": args.max_in_flight,
        "retries": 1,
        "enable.idempotence": False,
        "socket.keepalive.enable": True,
        "request.timeout.ms": 120000,
        "message.timeout.ms": 300000,
    }
    p = Producer(conf)

    def cb(err, _msg):
        pass

    def throttler(rate):
        if rate <= 0:
            return lambda: None
        interval = 1.0 / float(rate)
        next_t = [time.time()]
        def _tick():
            t = time.time()
            if t < next_t[0]:
                time.sleep(next_t[0] - t)
            next_t[0] += interval
        return _tick

    tick = throttler(args.rate)
    dataset_id = os.path.basename(args.file)

    sent = 0
    total_bytes = 0
    t0 = time.time()
    with open(args.file, "rb") as f:
        for raw in f:
            ts_read = int(time.time() * 1000)
            payload = raw.rstrip(b"\r\n")
            if not payload:
                continue
            headers = None
            if not args.no_headers:
                ts_prod = int(time.time() * 1000)
                headers = [
                    ("eid", uuid.uuid4().hex.encode()),
                    ("ts_read_start", str(ts_read).encode()),
                    ("ts_produce", str(ts_prod).encode()),
                    ("sz", str(len(payload)).encode()),
                    ("ds", dataset_id.encode()),
                    ("src.app", b"analysis-file-producer"),
                ]
            while True:
                try:
                    p.produce(topic=args.topic, value=payload, headers=headers, on_delivery=cb)
                    break
                except BufferError:
                    p.poll(0.005)
            sent += 1
            total_bytes += len(payload)
            if (sent % 5000) == 0:
                p.poll(0)
            tick()

    p.flush()
    dt = max(1e-6, time.time() - t0)
    mbps = (total_bytes / (1024 * 1024)) / dt
    print(f"Sent {sent} msgs ({total_bytes/1024/1024:.2f} MB) in {dt:.2f}s -> {mbps:.2f} MB/s")


if __name__ == "__main__":
    main()


