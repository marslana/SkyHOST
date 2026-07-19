#!/usr/bin/env python3
"""End-to-end latency + throughput measurement for Kafka-to-Kafka transfers.

Consumes the destination topic and computes per-message end-to-end latency as
(receive_wall_ms - ts_produce_ms), where ts_produce is the per-message header
stamped by the source producer (see analysis/confluent_file_producer.py).

REQUIRES synchronized clocks on the source and destination VMs (e.g. chrony
against the AWS Time Sync service), since ts_produce is on the source clock and
the receive timestamp is on the destination clock.
"""
import argparse
import csv
import time
from confluent_kafka import Consumer


def _hdr(headers, key):
    if not headers:
        return None
    for name, val in headers:
        if name == key:
            return None if val is None else (val.decode() if isinstance(val, (bytes, bytearray)) else str(val))
    return None


def percentile(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = (q / 100.0) * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def main():
    ap = argparse.ArgumentParser(description="Compute end-to-end latency and throughput using per-message ts_produce headers")
    ap.add_argument("--brokers", required=True, help="Destination broker(s), e.g., 127.0.0.1:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--dataset-id", required=True, help="Dataset id (producer 'ds' header, typically the filename)")
    ap.add_argument("--target-gb", type=float, default=1e9, help="Stop after consuming this many GB (default: effectively unbounded)")
    ap.add_argument("--target-messages", type=int, default=0, help="Stop after this many matching messages (0 = ignore)")
    ap.add_argument("--idle-timeout", type=float, default=30.0, help="Stop after this many seconds with no new matching messages")
    ap.add_argument("--warmup-frac", type=float, default=0.1, help="Fraction of earliest messages to drop before computing latency stats")
    ap.add_argument("--group-id", default=f"lat-{int(time.time())}")
    ap.add_argument("--save", default=None, help="Optional CSV path to write per-message records")
    ap.add_argument("--summary", default=None, help="Optional CSV path to APPEND a one-line metrics summary for aggregation")
    ap.add_argument("--label", default="", help="Config label recorded in the summary row (e.g. 'P4' or '100KB')")
    ap.add_argument("--rep", type=int, default=0, help="Repetition index recorded in the summary row")
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": args.group_id,
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
    records = []  # (recv_ms, ts_produce_ms, latency_ms, size, eid)
    first_ts_produce_ms = None
    start_wall = time.time()
    last_match_wall = time.time()

    try:
        while bytes_sum < target_bytes:
            m = c.poll(0.2)
            now = time.time()
            if not m or m.error():
                if (now - last_match_wall) >= args.idle_timeout and records:
                    break
                continue
            v = m.value()
            if not v:
                continue
            ds = _hdr(m.headers(), "ds")
            if ds != args.dataset_id:
                continue
            recv_ms = now * 1000.0
            tp = _hdr(m.headers(), "ts_produce")
            eid = _hdr(m.headers(), "eid")
            size = len(v)
            latency_ms = None
            if tp is not None:
                try:
                    tp_ms = int(tp)
                    latency_ms = recv_ms - tp_ms
                    if first_ts_produce_ms is None or tp_ms < first_ts_produce_ms:
                        first_ts_produce_ms = tp_ms
                except Exception:
                    pass
            records.append((recv_ms, int(tp) if tp else 0, latency_ms, size, eid))
            bytes_sum += size
            last_match_wall = now
            if args.target_messages and len(records) >= args.target_messages:
                break
    finally:
        end_wall = time.time()
        c.close()

    if not records:
        print("No matching messages observed for this dataset.")
        return

    # Drop warmup by receive order.
    records.sort(key=lambda r: r[0])
    drop = int(len(records) * max(0.0, min(0.9, args.warmup_frac)))
    steady = records[drop:]
    lats = sorted([r[2] for r in steady if r[2] is not None])

    # Throughput is measured over the actual data window, NOT the consumer's full
    # lifetime: the loop only exits after idle_timeout seconds of no new messages,
    # and the consumer is typically started before the producer, so end_wall-start_wall
    # would include both that leading idle and the trailing idle-timeout drain and
    # severely understate throughput. We therefore use the receive timestamps of the
    # first and last delivered messages (recv-window), and first-produce -> last-receive
    # for the true end-to-end rate.
    mb = bytes_sum / (1024 * 1024)
    recv_ms_vals = [r[0] for r in records]
    first_recv_ms, last_recv_ms = min(recv_ms_vals), max(recv_ms_vals)
    recv_sec = max(1e-6, (last_recv_ms - first_recv_ms) / 1000.0)
    recv_mbps = mb / recv_sec
    e2e_line = ""
    if first_ts_produce_ms is not None:
        e2e_sec = max(1e-6, (last_recv_ms - first_ts_produce_ms) / 1000.0)
        e2e_mbps = mb / e2e_sec
        e2e_line = f"  true-e2e throughput: {e2e_mbps:.2f} MB/s over {e2e_sec:.2f}s (first produce -> last receive)\n"

    print("=== Kafka-to-Kafka end-to-end results ===")
    print(f"  messages: {len(records)} (steady-state after {drop} warmup), bytes: {mb:.2f} MB")
    print(f"  recv-window throughput: {recv_mbps:.2f} MB/s over {recv_sec:.2f}s (first -> last receive)")
    print(e2e_line, end="")
    if lats:
        print(f"  latency ms  mean={sum(lats)/len(lats):.1f}  p50={percentile(lats,50):.1f}  "
              f"p90={percentile(lats,90):.1f}  p95={percentile(lats,95):.1f}  "
              f"p99={percentile(lats,99):.1f}  max={lats[-1]:.1f}")
    else:
        print("  latency: no ts_produce headers observed (check producer + header preservation)")

    if args.save:
        with open(args.save, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["recv_ms", "ts_produce_ms", "latency_ms", "size_bytes", "eid"])
            for r in records:
                w.writerow(r)
        print(f"  per-message records written to {args.save}")

    if args.summary:
        import os
        p50 = percentile(lats, 50) if lats else float("nan")
        p90 = percentile(lats, 90) if lats else float("nan")
        p95 = percentile(lats, 95) if lats else float("nan")
        p99 = percentile(lats, 99) if lats else float("nan")
        mean_lat = (sum(lats) / len(lats)) if lats else float("nan")
        max_lat = lats[-1] if lats else float("nan")
        e2e_mbps_val = (mb / max(1e-6, (last_recv_ms - first_ts_produce_ms) / 1000.0)) if first_ts_produce_ms is not None else float("nan")
        new = not os.path.exists(args.summary)
        with open(args.summary, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["label", "rep", "n_msgs", "mb", "recv_mbps", "e2e_mbps",
                            "mean_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms"])
            w.writerow([args.label, args.rep, len(records), f"{mb:.2f}", f"{recv_mbps:.2f}",
                        f"{e2e_mbps_val:.2f}", f"{mean_lat:.1f}", f"{p50:.1f}", f"{p90:.1f}",
                        f"{p95:.1f}", f"{p99:.1f}", f"{max_lat:.1f}"])
        print(f"  summary row appended to {args.summary} (label={args.label} rep={args.rep})")


if __name__ == "__main__":
    main()
