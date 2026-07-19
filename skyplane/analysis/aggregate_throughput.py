#!/usr/bin/env python3
import argparse
import csv
import statistics


def main():
    ap = argparse.ArgumentParser(description="Aggregate steady-state throughput from fast_validator CSV windows")
    ap.add_argument("--csv", required=True, help="Path to fast_validator output CSV")
    ap.add_argument("--warmup-windows", type=int, default=2, help="Drop this many initial windows as warmup")
    ap.add_argument("--min-windows", type=int, default=3, help="Require at least this many windows after warmup")
    args = ap.parse_args()

    mbps = []
    p50, p95, p99 = [], [], []
    with open(args.csv, newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        rows = rows[args.warmup_windows:]
        if len(rows) < args.min_windows:
            print("Not enough windows after warmup to compute steady-state.")
            return
        for row in rows:
            try:
                mbps.append(float(row["thr_mb_per_s"]))
                p50.append(float(row["e2e_p50_ms"]))
                p95.append(float(row["e2e_p95_ms"]))
                p99.append(float(row["e2e_p99_ms"]))
            except Exception:
                continue

    def summary(xs):
        return {
            "avg": statistics.mean(xs),
            "median": statistics.median(xs),
            "p10": statistics.quantiles(xs, n=10)[0],
            "p90": statistics.quantiles(xs, n=10)[-1],
        }

    s_mbps = summary(mbps)
    s_p50 = summary(p50)
    s_p95 = summary(p95)
    s_p99 = summary(p99)
    print("Steady-state throughput (MB/s):", s_mbps)
    print("Latency p50 (ms):", s_p50)
    print("Latency p95 (ms):", s_p95)
    print("Latency p99 (ms):", s_p99)


if __name__ == "__main__":
    main()


