#!/usr/bin/env python3
"""Header-agnostic destination-side throughput meter for S3->Kafka.

Measures end-to-end ingestion throughput on the destination topic identically for
SkyHOST and Confluent S3 Source Connector. Start this BEFORE launching the transfer
with a fresh consumer group and earliest offset; it counts every delivered record
and reports throughput over the delivery span (first arrival -> last arrival).
"""
import argparse
import time
import json
import os
from confluent_kafka import Consumer


def main():
    ap = argparse.ArgumentParser(description="S3->Kafka destination throughput meter (header-agnostic)")
    ap.add_argument("--brokers", required=True, help="Destination broker(s), e.g., 127.0.0.1:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--idle-timeout", type=float, default=30.0, help="Stop after this many seconds with no new records")
    ap.add_argument("--max-wait", type=float, default=1800.0, help="Hard cap on total wait (s)")
    ap.add_argument("--group-id", default=None, help="Consumer group id (default: fresh per run)")
    ap.add_argument("--from-beginning", action="store_true", help="Read from earliest offset (recommended)")
    ap.add_argument("--label", default="", help="Free-form label echoed into output")
    ap.add_argument("--save", default=None, help="Append a JSON line with the result to this path")
    args = ap.parse_args()

    group = args.group_id or f"s3thr-{int(time.time())}"
    c = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1,
        "fetch.max.bytes": 67108864,
        "max.partition.fetch.bytes": 67108864,
        "socket.keepalive.enable": True,
        "session.timeout.ms": 15000,
    })
    c.subscribe([args.topic])

    bytes_sum = 0
    msgs = 0
    t_first = None
    t_last = None
    # Produce-span: derived from each record's embedded broker/producer timestamp
    # (CreateTime). This is consumer-independent, so it stays valid even when a
    # single consumer cannot drain a fast producer (e.g. Confluent at ~800K msgs/s).
    prod_ts_min = None
    prod_ts_max = None
    last_activity = time.time()
    start_wall = time.time()

    try:
        while True:
            now = time.time()
            if t_first is not None and (now - last_activity) >= args.idle_timeout:
                break
            if (now - start_wall) >= args.max_wait:
                break
            m = c.poll(0.5)
            if m is None or m.error():
                continue
            v = m.value()
            k = m.key()
            sz = (len(v) if v else 0) + (len(k) if k else 0)
            now = time.time()
            if t_first is None:
                t_first = now
            t_last = now
            last_activity = now
            bytes_sum += sz
            msgs += 1
            try:
                ts_type, ts_ms = m.timestamp()
                if ts_ms is not None and ts_ms > 0:
                    if prod_ts_min is None or ts_ms < prod_ts_min:
                        prod_ts_min = ts_ms
                    if prod_ts_max is None or ts_ms > prod_ts_max:
                        prod_ts_max = ts_ms
            except Exception:
                pass
    finally:
        c.close()

    if t_first is None or t_last is None or msgs == 0:
        print(f"[{args.label}] No records observed on topic {args.topic}.")
        return

    span = max(1e-6, t_last - t_first)
    mbps = (bytes_sum / (1024 * 1024)) / span
    mps = msgs / span

    # Consumer-independent produce-span throughput (preferred for fast producers)
    prod_span = None
    prod_mbps = None
    prod_mps = None
    if prod_ts_min is not None and prod_ts_max is not None and prod_ts_max > prod_ts_min:
        prod_span = (prod_ts_max - prod_ts_min) / 1000.0
        prod_mbps = (bytes_sum / (1024 * 1024)) / prod_span
        prod_mps = msgs / prod_span

    print(f"[{args.label}] RECV {bytes_sum/1024/1024:.2f} MB, {msgs} msgs | "
          f"recv-span {span:.2f}s -> {mbps:.2f} MB/s, {mps:.0f} msgs/s"
          + (f" | produce-span {prod_span:.2f}s -> {prod_mbps:.2f} MB/s, {prod_mps:.0f} msgs/s"
             if prod_span is not None else " | produce-span n/a"))

    if args.save:
        rec = {
            "label": args.label,
            "topic": args.topic,
            "bytes": bytes_sum,
            "msgs": msgs,
            "span_s": round(span, 3),
            "t_first_epoch": round(t_first, 3),
            "t_last_epoch": round(t_last, 3),
            "throughput_MBps_span": round(mbps, 3),
            "msgs_per_s": round(mps, 1),
            "produce_span_s": round(prod_span, 3) if prod_span is not None else None,
            "produce_MBps": round(prod_mbps, 3) if prod_mbps is not None else None,
            "produce_msgs_per_s": round(prod_mps, 1) if prod_mps is not None else None,
            "ts": int(time.time()),
        }
        need_dir = os.path.dirname(args.save)
        if need_dir:
            os.makedirs(need_dir, exist_ok=True)
        with open(args.save, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")


if __name__ == "__main__":
    main()
