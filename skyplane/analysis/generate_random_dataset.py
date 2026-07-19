#!/usr/bin/env python3
import argparse
import os
import secrets


def main():
    ap = argparse.ArgumentParser(description="Generate an incompressible line-oriented dataset (random bytes per line)")
    ap.add_argument("--output", required=True, help="Output file path (e.g., /tmp/ds_10KB_3GB.jsonl)")
    ap.add_argument("--line-bytes", type=int, required=True, help="Bytes per line (payload only; newline added separately)")
    ap.add_argument("--total-gb", type=float, default=3.0, help="Total dataset size in GB (default: 3.0)")
    args = ap.parse_args()

    total_bytes_target = int(args.total_gb * 1024 * 1024 * 1024)
    payload_len = max(1, args.line_bytes)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)

    written = 0
    with open(args.output, "wb") as f:
        while written < total_bytes_target:
            payload = secrets.token_bytes(payload_len)
            f.write(payload + b"\n")
            written += len(payload) + 1

    print(f"Wrote {written/1024/1024/1024:.2f} GB to {args.output} (line_bytes={payload_len})")


if __name__ == "__main__":
    main()


