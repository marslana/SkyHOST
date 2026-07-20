# SkyHOST

**SkyHOST** is a unified cross-cloud data movement framework that extends
[Skyplane](https://github.com/skyplane-project/skyplane) with native support for
**streaming systems**. In addition to Skyplane's object-store-to-object-store
transfers, SkyHOST moves data **into and between Apache Kafka clusters** across
cloud regions and providers, using ephemeral gateway VMs and parallel TCP
connections over the WAN.

SkyHOST is built on the Skyplane core (Apache License 2.0). The transfer engine,
provisioner, and internal APIs retain Skyplane's naming; SkyHOST adds the Kafka
data plane, the record-aware object-store source, and the unified CLI on top.

## What SkyHOST adds

- **Kafka-to-Kafka streaming** (`skyhost stream`): continuously replicate a
  source Kafka topic to a destination topic in another region/cloud through a
  gateway pair, with micro-batching, partition preservation, and tunable
  destination producer semantics (`acks`, idempotence, in-flight requests).
- **Object store to Kafka** (`skyhost transfer s3://... kafka://...`):
  - *Byte-sliced (chunk) mode* for arbitrary objects: objects are split into
    chunks, shipped over parallel WAN connections, and produced to Kafka as
    bounded-size messages.
  - *Record-aware mode* for `.csv` objects: the source gateway parses records
    and emits one Kafka record per line (SKMETA01 framing), so the destination
    topic receives well-formed records rather than byte slices. An optional
    minimal-framing mode (`--src-minimal-framing --dst-minimal-headers`)
    removes per-record header overhead for high-throughput streaming.
- **Unified `transfer` command**: routes to `cp` or `stream` automatically
  based on the URI schemes (`s3://`, `kafka://`, ...).
- **At-least-once delivery** for Kafka paths via manual offset commits at the
  source and acknowledged produces at the destination.

All existing Skyplane functionality (`cp`, `sync` between object stores) is
unchanged and available under the same CLI.

## Installation

Requires Python 3.8–3.11 (Python 3.12+ is not supported yet).

```bash
git clone https://github.com/marslana/SkyHOST.git
cd SkyHOST
python3.11 -m venv venv && source venv/bin/activate
pip install -e ".[aws]"
skyhost init   # configure cloud credentials
```

The gateways run a prebuilt Docker image
(`ghcr.io/arslan866/skyplane-kafka:amd64-minframe`) which is pulled
automatically when a transfer is launched. Override it with the
`SKYPLANE_DOCKER_IMAGE` environment variable if you build your own image
(see `Dockerfile.skyplane-dev`).

## Quickstart

All transfer modes are available through the unified `skyhost transfer`
command, which routes to the right data plane based on the URI schemes
(`skyhost stream` and `skyhost cp` remain available as explicit aliases).

Kafka-to-Kafka streaming between two regions:

```bash
skyhost transfer "kafka://SRC_BROKER:9092/src_topic" "kafka://DST_BROKER:9092/dst_topic" \
  --src-region aws:us-east-1 --dst-region aws:eu-central-1 \
  --reader-processes 4 --writer-processes 4 --send-connections 4 \
  --batch-size-mb 32 --preserve-partitions \
  --dst-acks 1 --no-dst-idempotence -y
```

S3 to Kafka, byte-sliced (any object):

```bash
skyhost transfer "s3://my-bucket/data.bin" "kafka://DST_BROKER:9092/my_topic" \
  --src-region aws:eu-central-1 --dst-region aws:us-east-1 \
  --src-chunk-mb 32 --src-readers 4 --send-connections 4 --dst-writers 4 -y
```

S3 to Kafka, record-aware (CSV, one Kafka record per line):

```bash
skyhost transfer "s3://my-bucket/dataset/" "kafka://DST_BROKER:9092/my_topic" \
  --src-region aws:eu-central-1 --dst-region aws:us-east-1 --recursive \
  --src-csv-batch-mb 8 --src-readers 4 --send-connections 4 --dst-writers 4 \
  --src-minimal-framing --dst-minimal-headers -y
```

Object store to object store (inherited from Skyplane):

```bash
skyhost cp -r s3://src-bucket/prefix/ gs://dst-bucket/prefix/
```

Tear down any gateways left running:

```bash
skyhost deprovision
```

## Measurement utilities

`skyplane/analysis/` contains the standalone tools used to benchmark transfers
on the broker hosts: dataset generators, a max-throughput file-backed producer,
and destination-side end-to-end throughput/latency meters that read the
per-message produce timestamps stamped by the producer.

## Notes

- SkyHOST provisions VMs and transfers data across clouds: **egress and
  instance charges apply**. Always confirm `skyhost deprovision` at the end of
  a session.
- The legacy `skyplane` command remains available as an alias of `skyhost`.

## License

Apache License 2.0. SkyHOST is a fork of
[Skyplane](https://github.com/skyplane-project/skyplane) (UC Berkeley Sky
Computing Lab); see `LICENSE` and the upstream repository for the original
project and its contributors.
