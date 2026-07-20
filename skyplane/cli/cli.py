import subprocess
from shlex import split
from typing import Optional

import typer
from rich.prompt import IntPrompt

import skyplane.cli.cli_cloud
import skyplane.cli.cli_config

# import skyplane.cli.experiments # disable experiments
from skyplane import compute

from skyplane.cli.cli_init import init
from skyplane.cli.cli_transfer import cp, sync
from skyplane.cli.cli_stream import stream
from skyplane.cli.impl.common import query_instances
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
from skyplane.utils.path import parse_path
from skyplane.config_paths import cloud_config
from skyplane.api.dataplane import Dataplane
from skyplane.api.config import TransferConfig, AWSConfig, AzureConfig, GCPConfig, IBMCloudConfig, SCPConfig
from skyplane.planner.topology import TopologyPlan
from skyplane.gateway.gateway_program import GatewayProgram, GatewayReadObjectStore, GatewaySend, GatewayReceive, GatewayWriteKafka
from skyplane.api.transfer_job import S3ToKafkaJob
import time

app = typer.Typer(name="skyhost")
app.command(
    name="cp",
    help="Copy files between any two cloud object stores",
)(cp)
app.command(
    name="sync",
    help="Sync files between any two cloud object stores",
)(sync)
app.command(
    name="stream",
    help="Continuously stream data from a source Kafka topic to a destination Kafka topic",
)(stream)
@app.command(name="transfer", help="Unified transfer: routes to cp or stream based on URI schemes")
def transfer(
    src: str = typer.Argument(..., help="Source path e.g. s3://bucket/prefix or kafka://<brokers>/<topic>"),
    dst: str = typer.Argument(..., help="Destination path e.g. s3://bucket/prefix or kafka://<brokers>/<topic>"),
    # cp options
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Recursively copy object prefixes"),
    debug: bool = typer.Option(False, help="Write debug information to local directory"),
    multipart: bool = typer.Option(cloud_config.get_flag("multipart_enabled"), help="Use multipart uploads (cp only)"),
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all prompts"),
    max_instances: int = typer.Option(cloud_config.get_flag("max_instances"), "--max-instances", "-n", help="Gateways per region"),
    max_connections: int = typer.Option(cloud_config.get_flag("num_connections"), "--max-connections", help="Connections per gateway (cp only)"),
    solver: str = typer.Option("direct", "--solver", help="Solver for object-store transfers (cp only)"),
    solver_required_throughput_gbits: float = typer.Option(1.0, "--tput", "-t", help="Required throughput in Gbps (cp only)"),
    # stream options (used only if both URIs are kafka://)
    src_region: Optional[str] = typer.Option(None, "--src-region", help="Source region for streaming (e.g., aws:us-east-1)"),
    dst_region: Optional[str] = typer.Option(None, "--dst-region", help="Destination region for streaming (e.g., gcp:us-central1)"),
    reader_processes: int = typer.Option(1, "--reader-processes", help="Kafka reader processes (stream only)"),
    writer_processes: int = typer.Option(4, "--writer-processes", help="Kafka writer processes (stream only)"),
    batch_size_mb: int = typer.Option(32, "--batch-size-mb", help="Target batch size in MB for the Kafka reader (stream only)"),
    batch_max_time_ms: int = typer.Option(50, "--batch-max-time-ms", help="Max time in ms before sending a partial batch (stream only)"),
    batch_max_messages: int = typer.Option(10000, "--batch-max-messages", help="Max records per batch from the Kafka reader (stream only)"),
    preserve_partitions: bool = typer.Option(True, "--preserve-partitions/--no-preserve-partitions", help="Preserve source partition numbers at destination (stream only)"),
    # Destination Kafka tuning and local sink (stream only)
    dst_kafka_acks: Optional[str] = typer.Option(None, "--dst-acks", help="Kafka producer acks at destination: all, 1, or 0 (stream only)"),
    dst_enable_idempotence: Optional[bool] = typer.Option(None, "--dst-idempotence/--no-dst-idempotence", help="Enable idempotent producer at destination (stream only)"),
    dst_max_in_flight: Optional[int] = typer.Option(None, "--dst-max-in-flight", help="Max in-flight requests per connection at destination producer (stream only)"),
    dst_local_sink: Optional[str] = typer.Option(None, "--dst-local-sink", help="If set, write received batches to this local folder on DGW instead of Kafka (stream only)"),
    # S3->Kafka tuning (writer/reader workers, partitioning and producer)
    src_readers: int = typer.Option(32, "--src-readers", help="Source object-store reader processes (S3->Kafka)"),
    send_connections: int = typer.Option(32, "--send-connections", help="Source network sender processes (S3->Kafka)"),
    dst_writers: int = typer.Option(32, "--dst-writers", help="Destination Kafka writer processes (S3->Kafka)"),
    dst_preserve_partitions: bool = typer.Option(True, "--dst-preserve-partitions/--no-dst-preserve-partitions", help="Preserve source-derived partitioning at destination (S3->Kafka)"),
    dst_max_message_bytes: int = typer.Option(800 * 1024, "--dst-max-message-bytes", help="Max bytes per Kafka message slice at destination (S3->Kafka)"),
    dst_linger_ms: int = typer.Option(100, "--dst-linger-ms", help="Kafka producer linger.ms at destination (S3->Kafka)"),
    dst_minimal_headers: bool = typer.Option(False, "--dst-minimal-headers/--no-dst-minimal-headers", help="Emit minimal per-record headers at destination (S3->Kafka)"),
    src_minimal_framing: bool = typer.Option(False, "--src-minimal-framing/--no-src-minimal-framing", help="Source emits value-only SKMETA01 frames with zero per-record headers (S3->Kafka CSV record-aware); removes per-record header-build cost for high-throughput streaming"),
    src_chunk_mb: int = typer.Option(8, "--src-chunk-mb", help="Source chunk size in MiB for S3 reads and TCP transfer (S3->Kafka)"),
    # CSV record-aware batching controls (S3->Kafka, applied when source key ends with .csv)
    src_csv_max_records: Optional[int] = typer.Option(None, "--src-csv-max-records", help="Max CSV records per batch at source (S3->Kafka; record-safe)."),
    src_csv_batch_mb: Optional[int] = typer.Option(None, "--src-csv-batch-mb", help="Target CSV batch size in MiB at source (S3->Kafka; record-safe)."),
    no_deprovision: bool = typer.Option(False, "--no-deprovision", help="Keep gateways running after job completes (S3->Kafka only)"),
):
    """Dispatch to cp or stream based on URI schemes without changing existing behaviors."""
    src_provider, _, _ = parse_path(src)
    dst_provider, _, _ = parse_path(dst)

    is_kafka = lambda p: str(p).lower() == "kafka"
    if is_kafka(src_provider) and is_kafka(dst_provider):
        # Route to streaming
        if not src_region or not dst_region:
            typer.secho("--src-region and --dst-region are required for Kafka streaming.", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2)
        # Pass every parameter explicitly: calling the typer-decorated stream()
        # directly would otherwise leave unpassed params as OptionInfo objects.
        # Unset shared options fall back to the same defaults as `skyhost stream`.
        return stream(
            src=src,
            dst=dst,
            src_region=src_region,
            dst_region=dst_region,
            debug=debug,
            confirm=confirm,
            max_instances=max_instances,
            reader_processes=reader_processes,
            writer_processes=writer_processes,
            send_connections=send_connections,
            batch_size_mb=batch_size_mb,
            batch_max_time_ms=batch_max_time_ms,
            batch_max_messages=batch_max_messages,
            preserve_partitions=preserve_partitions,
            dst_kafka_acks=(dst_kafka_acks or "1"),
            dst_enable_idempotence=(False if dst_enable_idempotence is None else dst_enable_idempotence),
            dst_max_in_flight=(5 if dst_max_in_flight is None else dst_max_in_flight),
            dst_linger_ms=dst_linger_ms,
            dst_local_sink=dst_local_sink,
        )

    if is_kafka(src_provider) or is_kafka(dst_provider):
        # Minimal S3->Kafka route
        if is_kafka(dst_provider) and src_provider in {"aws", "gcp", "azure", "ibmcloud", "cloudflare", "scp", "local"}:
            # parse kafka
            _, dst_brokers, dst_topic = parse_path(dst)
            if not src_region or not dst_region:
                typer.secho("--src-region and --dst-region are required for S3->Kafka.", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=2)
            # Initialize client
            transfer_config = TransferConfig(
                autoterminate_minutes=cloud_config.get_flag("autoshutdown_minutes"),
                requester_pays=cloud_config.get_flag("requester_pays"),
                use_bbr=cloud_config.get_flag("bbr"),
                use_compression=cloud_config.get_flag("compress"),
                use_e2ee=cloud_config.get_flag("encrypt_e2e"),
                aws_use_spot_instances=cloud_config.get_flag("aws_use_spot_instances"),
                azure_use_spot_instances=cloud_config.get_flag("azure_use_spot_instances"),
                gcp_use_spot_instances=cloud_config.get_flag("gcp_use_spot_instances"),
            )
            import skyplane
            client = skyplane.SkyplaneClient(
                aws_config=AWSConfig() if cloud_config.aws_enabled else None,
                azure_config=AzureConfig() if cloud_config.azure_enabled else None,
                gcp_config=GCPConfig() if cloud_config.gcp_enabled else None,
                ibmcloud_config=IBMCloudConfig() if cloud_config.ibmcloud_enabled else None,
                scp_config=SCPConfig() if cloud_config.scp_enabled else None,
                transfer_config=transfer_config
            )
            provisioner = client.provisioner
            transfer_config = client.transfer_config

            topo = TopologyPlan(src_region_tag=src_region, dest_region_tags=[dst_region])
            default_transfer_config = TransferConfig()
            src_provider_tag, _ = src_region.split(":")
            dst_provider_tag, _ = dst_region.split(":")
            src_vm_type = cloud_config.get_flag(f"{src_provider_tag}_instance_class") or getattr(default_transfer_config, f"{src_provider_tag}_instance_class")
            dst_vm_type = cloud_config.get_flag(f"{dst_provider_tag}_instance_class") or getattr(default_transfer_config, f"{dst_provider_tag}_instance_class")
            sgw = topo.add_gateway(src_region, src_vm_type)
            dgw = topo.add_gateway(dst_region, dst_vm_type)

            # Build programs
            src_prog = GatewayProgram()
            # Read from object store; pass the actual bucket name
            _, src_bucket, _ = parse_path(src)
            # Optional CSV batching flags: only applied if the object ends with .csv
            _csv_bytes = (src_csv_batch_mb * 1024 * 1024) if src_csv_batch_mb is not None else None
            # End-to-end minimal mode: --dst-minimal-headers also enables source-side minimal framing
            # so the per-record header construction cost is removed at the source, not just dropped at delivery.
            _csv_minimal = bool(src_minimal_framing or dst_minimal_headers)
            h_read = src_prog.add_operator(
                GatewayReadObjectStore(
                    bucket_name=src_bucket,
                    bucket_region=src_region,
                    num_connections=src_readers,
                    csv_target_batch_bytes=_csv_bytes,
                    csv_max_records_per_batch=src_csv_max_records,
                    csv_minimal_framing=_csv_minimal,
                ),
                partition_id="s3_to_kafka",
            )
            # Disable on-wire compression for S3->Kafka
            src_prog.add_operator(GatewaySend(target_gateway_id=dgw.gateway_id, region=dst_region, num_connections=send_connections, compress=False), parent_handle=h_read, partition_id="s3_to_kafka")
            sgw.set_gateway_program(src_prog)

            dst_prog = GatewayProgram()
            # Disable decompression (wire compression is disabled)
            h_recv = dst_prog.add_operator(GatewayReceive(decompress=False), partition_id="s3_to_kafka")
            # Destination Kafka writer with user-tunable options
            dst_prog.add_operator(GatewayWriteKafka(
                kafka_brokers=dst_brokers,
                topic=dst_topic,
                num_connections=dst_writers,
                preserve_partitions=dst_preserve_partitions,
                acks=(dst_kafka_acks or "all"),
                enable_idempotence=(True if dst_enable_idempotence is None else dst_enable_idempotence),
                max_in_flight_requests_per_connection=(1 if dst_max_in_flight is None else dst_max_in_flight),
                max_message_bytes=dst_max_message_bytes,
                linger_ms=dst_linger_ms,
                minimal_headers=dst_minimal_headers,
            ), parent_handle=h_recv, partition_id="s3_to_kafka")
            dgw.set_gateway_program(dst_prog)

            dp = Dataplane(client.clientid, topo, provisioner, transfer_config, client.log_dir, debug=debug)

            # Dispatch S3->Kafka job with user-configurable source chunk size
            job = S3ToKafkaJob(src_path=src, dst_path=dst, chunk_size_mb=src_chunk_mb)
            try:
                if no_deprovision:
                    # Manual lifecycle so the user can observe Kafka offsets before tearing down
                    dp.provision(spinner=False)
                    tracker = dp.run_async([job])
                    tracker.join()
                    if tracker.errors:
                        typer.secho(f"Transfer encountered errors: {tracker.errors}", fg=typer.colors.RED, err=True)
                        raise typer.Exit(code=1)
                    typer.secho("Gateways are left running (--no-deprovision). Run 'skyhost deprovision' when done.", fg=typer.colors.YELLOW)
                else:
                    with dp.auto_deprovision():
                        dp.provision(spinner=False)
                        tracker = dp.run_async([job])
                        tracker.join()
                        if tracker.errors:
                            typer.secho(f"Transfer encountered errors: {tracker.errors}", fg=typer.colors.RED, err=True)
                            raise typer.Exit(code=1)
            except KeyboardInterrupt:
                pass
            return
        else:
            typer.secho("Mixed Kafka and object store transfers are not supported yet for this direction.", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2)

    # Route to cp
    return cp(
        src=src,
        dst=dst,
        recursive=recursive,
        debug=debug,
        multipart=multipart,
        confirm=confirm,
        max_instances=max_instances,
        max_connections=max_connections,
        solver=solver,
        solver_required_throughput_gbits=solver_required_throughput_gbits,
    )

app.command(
    name="init",
    help="Initialize the SkyHOST CLI with your cloud credentials",
)(init)
# app.add_typer(skyplane.cli.experiments.app, name="experiments") # disable experiments
app.add_typer(skyplane.cli.cli_cloud.app, name="cloud")
app.add_typer(skyplane.cli.cli_config.app, name="config")


@app.command()
def deprovision(
    all: bool = typer.Option(False, "--all", "-a", help="Deprovision all resources including networks."),
    filter_client_id: Optional[str] = typer.Option(None, help="Only deprovision instances with this client ID under the instance tag."),
):
    """Deprovision all resources created by SkyHOST."""
    instances = query_instances()
    if filter_client_id:
        instances = [instance for instance in instances if instance.tags().get("skyplaneclientid") == filter_client_id]

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, desc="Deprovisioning", spinner=True, spinner_persist=True)
        # if compute.SCPAuthentication().enabled():
        if any([instance.region_tag.split(":")[0] == "scp" for instance in instances]):
            typer.secho(f"Removing SCP Gateway Rules", fg="yellow", bold=True)
            scp_instances = [instance for instance in instances if instance.region_tag.split(":")[0] == "scp"]
            scp = compute.SCPCloudProvider()
            scp.remove_gateway_rule_all(scp_instances)
    else:
        typer.secho("No instances to deprovision", fg="yellow", bold=True)

    if all:
        if compute.AWSAuthentication().enabled():
            aws = compute.AWSCloudProvider()
            aws.teardown_global()
        if compute.GCPAuthentication().enabled():
            gcp = compute.GCPCloudProvider()
            gcp.teardown_global()
        if compute.AzureAuthentication().enabled():
            azure = compute.AzureCloudProvider()
            azure.teardown_global()
        if compute.SCPAuthentication().enabled():
            scp = compute.SCPCloudProvider()
            scp.teardown_global()


@app.command()
def ssh():
    """SSH into a running gateway."""
    instances = query_instances()
    if len(instances) == 0:
        typer.secho(f"No instances found", fg="red", err=True)
        raise typer.Abort()

    instance_map = {f"{i.region_tag}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))

    # ask for selection
    typer.secho("Select an instance:", fg="yellow", bold=True)
    for i, choice in enumerate(choices):
        typer.secho(f"{i+1}) {choice}", fg="yellow")
    choice = IntPrompt.ask("Enter an instance number", choices=list([str(i) for i in range(1, len(choices) + 1)]), show_choices=False)
    instance = instance_map[choices[choice - 1]]

    # ssh
    cmd = instance.get_ssh_cmd()
    logger.info(f"Running SSH command: {cmd}")
    logger.info("It may ask for a private key password, try `skyplane`.")
    proc = subprocess.Popen(split(cmd))
    proc.wait()


typer_click_object = typer.main.get_command(app)

if __name__ == "__main__":
    app()
