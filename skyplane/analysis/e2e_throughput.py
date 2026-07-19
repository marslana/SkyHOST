#!/usr/bin/env python3
import argparse
import time
from confluent_kafka import Consumer


def _hdr(headers, key):
    if not headers:
        return None
    for name, val in headers:
        if name == key:
            return None if val is None else (val.decode() if isinstance(val, (bytes, bytearray)) else str(val))
    return None


def main():
    ap = argparse.ArgumentParser(description="Compute true end-to-end throughput using producer timestamps in headers")
    ap.add_argument("--brokers", required=True, help="Destination broker(s), e.g., 127.0.0.1:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--dataset-id", required=True, help="Dataset id (producer 'ds' header, typically the filename)")
    ap.add_argument("--target-gb", type=float, default=3.0, help="Stop after consuming this many GB for the dataset")
    ap.add_argument("--group-id", default=lambda: f"e2e-{int(time.time())}")
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": (args.group_id if isinstance(args.group_id, str) else f"e2e-{int(time.time())}"),
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1048576,
        "fetch.max.bytes": 67108864,
        "max.partition.fetch.bytes": 67108864,
        "socket.keepalive.enable": True,
        "session.timeout.ms": 15000,
    })
    c.subscribe([args.topic])

    target_bytes = int(args.target_gb * 1024 * 1024 * 1024)
    bytes_sum = 0
    first_ts_produce_ms = None
    start_wall = time.time()

    try:
        while bytes_sum < target_bytes:
            m = c.poll(0.2)
            if not m or m.error():
                continue
            v = m.value()
            if not v:
                continue
            ds = _hdr(m.headers(), "ds")
            if ds != args.dataset_id:
                continue
            tp = _hdr(m.headers(), "ts_produce")
            if tp is not None:
                try:
                    tp_ms = int(tp)
                    if first_ts_produce_ms is None or tp_ms < first_ts_produce_ms:
                        first_ts_produce_ms = tp_ms
                except Exception:
                    pass
            bytes_sum += len(v)
    finally:
        end_wall = time.time()
        c.close()

    if first_ts_produce_ms is None:
        print("No ts_produce headers observed for this dataset; cannot compute true E2E.")
        return

    e2e_sec = max(1e-6, end_wall - (first_ts_produce_ms / 1000.0))
    mbps = (bytes_sum / (1024 * 1024)) / e2e_sec
    print(f"E2E: {bytes_sum/1024/1024:.2f} MB in {e2e_sec:.2f}s -> {mbps:.2f} MB/s")


if __name__ == "__main__":
    main()


