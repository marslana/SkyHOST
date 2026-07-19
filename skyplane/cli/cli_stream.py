# skyplane/cli/cli_stream.py

import uuid
import time
from typing import Optional
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
import skyplane
from skyplane.gateway.gateway_program import GatewayWriteLocal
from skyplane.api.dataplane import Dataplane
from skyplane.cli.impl.common import print_header, register_exception_handler
from skyplane.config_paths import cloud_config
from skyplane.api.config import TransferConfig
from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig, IBMCloudConfig, SCPConfig
from skyplane.gateway.gateway_program import (
    GatewayProgram,
    GatewayReadKafka,
    GatewaySend,
    GatewayReceive,
    GatewayWriteKafka,
)
from skyplane.planner.topology import TopologyPlan
from skyplane.api.transfer_job import StreamJob
from skyplane.api.usage import UsageClient
from skyplane.utils.path import parse_path


def parse_kafka_path(path: str) -> tuple[str, str, str]:
    """Parses a kafka path of the format kafka://<brokers>/<topic>"""
    provider, brokers, topic = parse_path(path)
    if provider.lower() != "kafka":
        raise ValueError(f"Invalid source or destination for stream: {path}. Must be a Kafka path, e.g. kafka://<broker_host>:<port>/<topic_name>")
    if not brokers or not topic:
         raise ValueError(f"Invalid Kafka path: {path}. Must specify brokers and topic.")
    return provider, brokers, topic


def stream(
    src: str = typer.Argument(..., help="Source Kafka in format: kafka://<brokers>/<topic>"),
    dst: str = typer.Argument(..., help="Destination Kafka in format: kafka://<brokers>/<topic>"),
    src_region: str = typer.Option(..., "--src-region", help="Source cloud region to provision a gateway (e.g., aws:us-east-1)"),
    dst_region: str = typer.Option(..., "--dst-region", help="Destination cloud region to provision a gateway (e.g., gcp:us-central1)"),
    debug: bool = typer.Option(False, help="If true, will write debug information to a local directory."),
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    max_instances: int = typer.Option(1, "--max-instances", "-n", help="Number of gateways per region (currently only 1 is supported for streaming)"),
    reader_processes: int = typer.Option(1, "--reader-processes", help="Number of parallel processes for the KafkaReadOperator."),
    writer_processes: int = typer.Option(4, "--writer-processes", help="Number of parallel processes for the KafkaWriteOperator."),
    send_connections: int = typer.Option(32, "--send-connections", help="Number of TCP connections used by the inter-gateway sender."),
    batch_size_mb: int = typer.Option(32, "--batch-size-mb", help="Target batch size in MB for Kafka reader before sending downstream."),
    batch_max_time_ms: int = typer.Option(50, "--batch-max-time-ms", help="Max time in ms to wait before sending a partial batch from the Kafka reader."),
    batch_max_messages: int = typer.Option(10000, "--batch-max-messages", help="Max records per batch from the Kafka reader."),
    preserve_partitions: bool = typer.Option(True, "--preserve-partitions/--no-preserve-partitions", help="Attempt to preserve source partition numbers on the destination topic when available."),
    # Destination Kafka producer tuning and local sink for bottleneck isolation
    dst_kafka_acks: str = typer.Option("1", "--dst-acks", help="Kafka producer acks at destination: all, 1, or 0"),
    dst_enable_idempotence: bool = typer.Option(False, "--dst-idempotence/--no-dst-idempotence", help="Enable idempotent producer at destination."),
    dst_max_in_flight: int = typer.Option(5, "--dst-max-in-flight", help="Max in-flight requests per connection at destination producer."),
    dst_linger_ms: int = typer.Option(100, "--dst-linger-ms", help="Kafka producer linger.ms at destination."),
    dst_local_sink: Optional[str] = typer.Option(None, "--dst-local-sink", help="If set, write received batches to this local folder on DGW instead of Kafka (bottleneck isolation)."),
):
    """
    `stream` continuously streams data from a source Kafka topic to a destination Kafka topic.

    This command provisions two gateway virtual machines, one in the source region and one in the destination region,
    to perform the transfer. Data is read from the source topic, transferred securely and efficiently over the public
    internet using Skyplane's transport, and then written to the destination topic.

    The process runs until you stop it with Ctrl+C, at which point the gateways are automatically deprovisioned.
    """
    if not debug:
        register_exception_handler()
    print_header()

    if max_instances > 1:
        typer.secho("Warning: Streaming currently only supports a single gateway per region (`--max-instances=1`). Using 1 gateway.", fg="yellow")
        max_instances = 1

    console = Console()

    try:
        _, src_brokers, src_topic = parse_kafka_path(src)
        _, dst_brokers, dst_topic = parse_kafka_path(dst)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)

    # Coerce CLI option values when this function is called programmatically (not via Typer)
    def _coerce(val, default, cast):
        try:
            return cast(val)
        except Exception:
            return default

    reader_processes = _coerce(reader_processes, 1, int)
    writer_processes = _coerce(writer_processes, 4, int)
    send_connections = _coerce(send_connections, 32, int)
    batch_size_mb = _coerce(batch_size_mb, 32, int)
    batch_max_time_ms = _coerce(batch_max_time_ms, 50, int)
    batch_max_messages = _coerce(batch_max_messages, 10000, int)
    if not isinstance(preserve_partitions, bool):
        preserve_partitions = True
    # Normalize acks input
    dst_kafka_acks = str(dst_kafka_acks).lower()
    if dst_kafka_acks not in {"all", "1", "0"}:
        dst_kafka_acks = "all"

    console.print(f"[bold yellow]Will stream from Kafka topic [blue]{src_topic}[/blue] to [blue]{dst_topic}[/blue][/bold yellow]")
    console.print(f"  [bold]Source:[/bold] {src_brokers} in [bold]{src_region}[/bold]")
    console.print(f"  [bold]Destination:[/bold] {dst_brokers} in [bold]{dst_region}[/bold]")
    
    if not confirm:
        if not Confirm.ask("\n[bold]Continue with the stream setup?[/bold]", default=True):
            console.print("[bold red]Aborted.[/bold red]")
            raise typer.Abort()

    with Progress(TextColumn(" "), SpinnerColumn(), TextColumn("{task.description}"), transient=True, console=console) as progress:
        progress.add_task("Initializing Skyplane client...", total=None)
        transfer_config = TransferConfig(
            # The keyword argument on the left must match the attribute name in the class definition.
            # The flag name on the right is what we look up in the config file.
            autoterminate_minutes=cloud_config.get_flag("autoshutdown_minutes"),
            requester_pays=cloud_config.get_flag("requester_pays"),
            use_bbr=cloud_config.get_flag("bbr"),
            use_compression=cloud_config.get_flag("compress"),
            use_e2ee=cloud_config.get_flag("encrypt_e2e"),
            aws_use_spot_instances=cloud_config.get_flag("aws_use_spot_instances"),
            azure_use_spot_instances=cloud_config.get_flag("azure_use_spot_instances"),
            gcp_use_spot_instances=cloud_config.get_flag("gcp_use_spot_instances"),
        )
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

        progress.add_task("Building streaming plan...", total=None)
        src_provider_tag, _ = src_region.split(":")
        dst_provider_tag, _ = dst_region.split(":")
        
        topo = TopologyPlan(src_region_tag=src_region, dest_region_tags=[dst_region])
        
        default_transfer_config = TransferConfig()
        src_vm_type = cloud_config.get_flag(f"{src_provider_tag}_instance_class") or getattr(default_transfer_config, f"{src_provider_tag}_instance_class")
        dst_vm_type = cloud_config.get_flag(f"{dst_provider_tag}_instance_class") or getattr(default_transfer_config, f"{dst_provider_tag}_instance_class")

        # --- START OF THE FINAL FIX ---
        # The add_gateway method generates its own ID. We only need to provide the region and the vm_type.
        # We also need to capture the returned gateway object to get its generated ID.
        sgw_gateway_obj = topo.add_gateway(src_region, src_vm_type)
        dgw_gateway_obj = topo.add_gateway(dst_region, dst_vm_type)
        
       
        # ... and to configure the GatewaySend operator
        src_prog = GatewayProgram()
        h_read = src_prog.add_operator(
            GatewayReadKafka(
                kafka_brokers=src_brokers,
                topic=src_topic,
                group_id=f"skyplane-src-{uuid.uuid4().hex[:8]}",
                num_connections=reader_processes,
                target_batch_size_mb=batch_size_mb,
                max_batch_time_seconds=batch_max_time_ms / 1000.0,
                max_messages_per_batch=batch_max_messages,
            ),
            partition_id="kafka_stream"
        )
        src_prog.add_operator(
            GatewaySend(target_gateway_id=dgw_gateway_obj.gateway_id, region=dst_region, num_connections=send_connections, compress=False),
            parent_handle=h_read,
            partition_id="kafka_stream"
        )
        sgw_gateway_obj.set_gateway_program(src_prog) # Use the object to set the program

        # Destination program
        dst_prog = GatewayProgram()
        h_recv = dst_prog.add_operator(GatewayReceive(decompress=True), partition_id="kafka_stream")
        if dst_local_sink:
            # Write locally on DGW for bottleneck isolation
            dst_prog.add_operator(
                GatewayWriteLocal(path=dst_local_sink, num_connections=writer_processes),
                parent_handle=h_recv,
                partition_id="kafka_stream"
            )
        else:
            dst_prog.add_operator(
                GatewayWriteKafka(
                    kafka_brokers=dst_brokers,
                    topic=dst_topic,
                    num_connections=writer_processes,
                    preserve_partitions=preserve_partitions,
                    acks=dst_kafka_acks,
                    enable_idempotence=dst_enable_idempotence,
                    max_in_flight_requests_per_connection=dst_max_in_flight,
                    linger_ms=dst_linger_ms,
                ),
                parent_handle=h_recv,
                partition_id="kafka_stream"
            )
        dgw_gateway_obj.set_gateway_program(dst_prog)

        dp = Dataplane(client.clientid, topo, provisioner, transfer_config, client.log_dir, debug=debug)
        
        try:
            with dp.auto_deprovision():
                progress.add_task(f"Provisioning gateways in {src_region} and {dst_region}...", total=None)
                dp.provision(spinner=False)
                
                progress.add_task("Starting stream...", total=None)
                stream_job = StreamJob(src_path=src, dst_paths=[dst])
                tracker = dp.run_async([stream_job])
                
                console.print(f"\n[green]✓ Stream initialized successfully.[/green]")
                console.print(f"[bright_black]  Logs are being saved to {client.log_dir}[/bright_black]")
                console.print(f"\n[bold]Streaming is now active. Press [yellow]Ctrl+C[/yellow] to stop and deprovision gateways.[/bold]")
                
                while not tracker.errors:
                    time.sleep(1)
                
                if tracker.errors:
                    console.print(f"[bold red]An error occurred during streaming:[/bold red]\n{tracker.errors}")

        except KeyboardInterrupt:
            console.print("\n\n[yellow]Keyboard interrupt detected. Stopping stream and deprovisioning gateways...[/yellow]")
        except Exception as e:
            console.print(f"[bold red]An unhandled exception occurred:[/bold red]\n{e}")
            UsageClient.log_exception("stream", e, {}, src_region, [dst_region])
        finally:
            console.print("[bold]Stream stopped.[/bold]")