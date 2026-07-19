#!/usr/bin/env python3
"""Generate a fixed-message-size JSONL dataset of a target total size.

Each line is a payload of exactly --size-bytes bytes (excluding the trailing
newline), so the source producer emits uniform messages. Used to drive the
Kafka-to-Kafka throughput/latency sweeps (1KB / 10KB / 100KB / 1000KB) at
larger total volumes (e.g. 5-10 GB) without changing message semantics.
"""
import argparse
import os
import sys


def main():
    ap = argparse.ArgumentParser(description="Generate fixed-size-message JSONL dataset")
    ap.add_argument("--out", required=True, help="Output file path")
    ap.add_argument("--size-bytes", type=int, required=True, help="Payload size per message in bytes (e.g. 1024)")
    ap.add_argument("--target-gb", type=float, required=True, help="Approximate total dataset size in GB")
    args = ap.parse_args()

    size = args.size_bytes
    if size < 16:
        print("size-bytes must be >= 16", file=sys.stderr)
        sys.exit(1)

    target_bytes = int(args.target_gb * 1024 * 1024 * 1024)
    # Account for the trailing newline per record.
    per_record = size + 1
    num = max(1, target_bytes // per_record)

    # Deterministic filler; first 16 bytes encode a zero-padded sequence number
    # so messages are distinguishable, the remainder is constant filler.
    filler = b"x" * (size - 16)
    written = 0
    with open(args.out, "wb") as f:
        for i in range(num):
            line = (b"%016d" % i) + filler + b"\n"
            f.write(line)
            written += len(line)

    print(f"Wrote {num} messages of {size} B -> {written/1024/1024/1024:.3f} GB to {args.out}")


if __name__ == "__main__":
    main()
