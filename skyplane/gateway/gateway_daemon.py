# gateway_daemon.py

print("<<<<< LOADING MODIFIED gateway_GW.py FILE - VERSION XYZ >>>>>")
import argparse
from multiprocessing import Manager
from multiprocessing import Queue as MPQueue
from pprint import pprint
import atexit
import json
import os
import signal
import sys
from multiprocessing import Event, Queue
from os import PathLike
from pathlib import Path
from typing import Dict, List

from skyplane.utils import logger

from skyplane.gateway.gateway_queue import GatewayQueue, GatewayANDQueue
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.gateway.gateway_daemon_api import GatewayDaemonAPI
# IMPORT ALL OPERATORS FROM BOTH VERSIONS
from skyplane.gateway.operators.gateway_operator import (
    GatewaySender,
    GatewayRandomDataGen,
    GatewayWriteLocal,
    GatewayObjStoreReadOperator,
    GatewayObjStoreWriteOperator,
    GatewayWaitReceiver,  # Restored from default
    GatewayKafkaReadOperator, # Kept from modified
    GatewayKafkaWriteOperator, # Kept from modified
)
from skyplane.gateway.operators.gateway_receiver import GatewayReceiver
from skyplane.utils import logger
from collections import defaultdict
from pprint import pprint
from collections import defaultdict
from pathlib import Path


class GatewayDaemon:
    def __init__(
        self,
        region: str,
        chunk_dir: PathLike,
        max_incoming_ports=64,
        use_tls=True,
        use_e2ee=False,
        use_compression=True,
    ):
        # read gateway program
        gateway_program_path = Path(os.environ["GATEWAY_PROGRAM_FILE"]).expanduser()
        gateway_program = json.load(open(gateway_program_path, "r"))

        self.upload_id_map = Manager().dict()
        self.write_stats_queue = MPQueue()

        pprint(gateway_program)

        # read gateway info
        gateway_info_path = Path(os.environ["GATEWAY_INFO_FILE"]).expanduser()
        self.gateway_info = json.load(open(gateway_info_path, "r"))

        print("starting gateway daemon", gateway_program_path)
        pprint(gateway_program)
        assert len(gateway_program) > 0, f"Cannot have empty gateway program {gateway_program}"

        self.use_tls = use_tls

        # todo max_incoming_ports should be configurable rather than static
        self.region = region
        self.max_incoming_ports = max_incoming_ports

        # the chunk store managest the incoming queue of chunks and outgoing queue of chunk status updates
        self.chunk_store = ChunkStore(chunk_dir)

        self.error_event = Event()
        self.error_queue = Queue()
        if use_e2ee:
            e2ee_key_path = Path(os.environ["E2EE_KEY_FILE"]).expanduser()
            with open(e2ee_key_path, "rb") as f:
                self.e2ee_key_bytes = f.read()
            print("Server side E2EE key loaded: ", self.e2ee_key_bytes)
        else:
            self.e2ee_key_bytes = None

        # create gateway operators
        self.terminal_operators = defaultdict(list)  # track terminal operators per partition
        self.num_required_terminal = {}
        self.operators = self.create_gateway_operators(gateway_program)

        # single gateway receiver
        self.gateway_receiver = GatewayReceiver(
            "reciever",
            region=region,
            chunk_store=self.chunk_store,
            error_event=self.error_event,
            error_queue=self.error_queue,
            max_pending_chunks=max_incoming_ports,
            use_tls=self.use_tls,
            use_compression=use_compression,
            e2ee_key_bytes=self.e2ee_key_bytes,
        )

        # API server
        self.api_server = GatewayDaemonAPI(
            self.chunk_store,
            self.gateway_receiver,
            self.error_event,
            self.error_queue,
            terminal_operators=self.terminal_operators,
            num_required_terminal=self.num_required_terminal,
            upload_id_map=self.upload_id_map,
        )
        self.api_server.start()
        atexit.register(self.api_server.shutdown)
        logger.info(f"[gateway_daemon] API started at {self.api_server.url}")

        # Start a background aggregator for local write stats
        def _aggregate_write_stats():
            import time as _t
            import threading as _th
            total_bytes = 0
            total_msgs = 0
            first_ts = None
            last_log_sec = -1
            while True:
                try:
                    evt = self.write_stats_queue.get()
                except Exception:
                    continue
                if not evt:
                    continue
                if evt.get("event") == "write":
                    total_bytes += int(evt.get("bytes", 0))
                    total_msgs += int(evt.get("msgs", 0))
                    if first_ts is None:
                        first_ts = evt.get("ts", _t.time())
                # Log every ~5 seconds
                now = int(_t.time())
                if first_ts is not None and now % 5 == 0 and now != last_log_sec:
                    last_log_sec = now
                    elapsed = max(1e-6, _t.time() - first_ts)
                    mbps = (total_bytes / (1024 * 1024)) / elapsed
                    logger.info(f"[gateway_daemon] Aggregate[local write]: msgs={total_msgs}, bytes={total_bytes}, avg_throughput={mbps:.2f} MB/s")

        import threading
        threading.Thread(target=_aggregate_write_stats, daemon=True).start()

    def print_operator_graph(self):
        def print_operator_graph_helper(partition, queue: GatewayQueue, prefix: str):
            if queue is None:
                return
            print(f"{prefix} {partition}: Input queue:", queue, "handles:", queue.get_handles())
            for handle in queue.get_handles():
                print(f"{prefix} {partition}: Operator {handle}")
                # TODO: causes error sometimes for mux_or
                next_queue = self.operators[handle].output_queue
                print_operator_graph_helper(partition, next_queue, prefix + "--")

        for partition, queue in self.chunk_store.chunk_requests.items():
            print(f"Partition {partition}, {queue}")
            print_operator_graph_helper(partition, queue, "")

    def create_gateway_operators(self, gateway_program: Dict):
        """Create a gateway plan from a gateway program"""

        operators = {}

        def create_output_queue(operator: Dict):
            # create output data queue
            if len(operator["children"]) == 0:
                return None
            print("get output queue", operator["op_type"], operator["children"][0]["op_type"])
            if operator["children"][0]["op_type"] == "mux_and":
                return GatewayANDQueue()
            return GatewayQueue(maxsize=5000)

        def get_child_operators(operator):
            if len(operator["children"]) == 0:
                return []
            if operator["children"][0]["op_type"] == "mux_and" or operator["children"][0]["op_type"] == "mux_or":
                return operator["children"][0]["children"]
            return operator["children"]

        def create_gateway_operators_helper(input_queue, program: List[Dict], partition_ids: List[str]):
            total_p = 0
            if not program or input_queue is None:
                logger.info("Helper called on terminal node or with empty program, returning.")
                return 0

            # --- Loop through all operators at this level of the DAG ---
            for op in program:
                handle = op["op_type"] + "_" + op["handle"]
                print(f"Input queue {input_queue}, adding handle {handle} (current handles {input_queue.get_handles()})")
                input_queue.register_handle(handle)

                child_operators = get_child_operators(op)

                # Handle mux_or, which is a special recursive case
                if op["op_type"] == "mux_or":
                    assert isinstance(input_queue, GatewayANDQueue), f"Parent must have been mux_and for {handle}"
                    total_p += create_gateway_operators_helper(input_queue.get_handle_queue(handle), child_operators, partition_ids)
                    continue  # Move to the next op in the 'program' list

                output_queue = create_output_queue(op)
                
                if isinstance(output_queue, GatewayANDQueue):
                    for partition in partition_ids:
                        self.num_required_terminal[str(partition)] = self.num_required_terminal.get(str(partition), 0) - 1 + len(child_operators)

                print(f"Input queue {input_queue} handle {handle}: created output queue {output_queue}")
                print(f"Input queue {input_queue} handle {handle}: has children {child_operators}")
                
                if output_queue is None:
                    for partition in partition_ids:
                        self.terminal_operators[str(partition)].append(handle)

                # --- MERGED OPERATOR CREATION LOGIC ---
                if op["op_type"] == "receive":
                    # THIS BLOCK IS RESTORED FROM YOUR WORKING 'cp' VERSION
                    operators[handle] = GatewayWaitReceiver(handle=handle, region=self.region, input_queue=input_queue, output_queue=output_queue, n_processes=1, chunk_store=self.chunk_store, error_event=self.error_event, error_queue=self.error_queue)
                    total_p += 1
                elif op["op_type"] == "read_object_store":
                    # Honor configured parallelism for object store reads
                    n_conn = int(op.get("num_connections", 1))
                    operators[handle] = GatewayObjStoreReadOperator(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_queue=self.error_queue,
                        error_event=self.error_event,
                        n_processes=n_conn,
                        chunk_store=self.chunk_store,
                        bucket_name=op["bucket_name"],
                        bucket_region=op["bucket_region"],
                        # Pass optional CSV settings if present
                        csv_target_batch_bytes=op.get("csv_target_batch_bytes"),
                        csv_max_records_per_batch=op.get("csv_max_records_per_batch"),
                        csv_minimal_framing=op.get("csv_minimal_framing", False),
                    )
                    total_p += n_conn
                elif op["op_type"] == "gen_data":
                    operators[handle] = GatewayRandomDataGen(handle=handle, region=self.region, input_queue=input_queue, output_queue=output_queue, error_queue=self.error_queue, error_event=self.error_event, chunk_store=self.chunk_store, size_mb=op["size_mb"])
                    total_p += 1
                elif op["op_type"] == "send":
                    target_gateway_info = self.gateway_info[op["target_gateway_id"]]
                    ip_addr = target_gateway_info["private_ip_address"] if op["private_ip"] else target_gateway_info["public_ip_address"]
                    print(f"Gateway sender sending to {ip_addr}, private={op['private_ip']}")
                    # Honor configured parallelism for network senders
                    n_conn = int(op.get("num_connections", 1))
                    operators[handle] = GatewaySender(
                        handle,
                        region=self.region,
                        ip_addr=ip_addr,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_event=self.error_event,
                        error_queue=self.error_queue,
                        chunk_store=self.chunk_store,
                        use_tls=self.use_tls,
                        use_compression=op["compress"],
                        e2ee_key_bytes=self.e2ee_key_bytes,
                        n_processes=n_conn,
                    )
                    total_p += n_conn
                elif op["op_type"] == "write_object_store":
                    operators[handle] = GatewayObjStoreWriteOperator(handle=handle, region=self.region, input_queue=input_queue, output_queue=output_queue, error_queue=self.error_queue, error_event=self.error_event, n_processes=op["num_connections"], chunk_store=self.chunk_store, bucket_name=op["bucket_name"], bucket_region=op["bucket_region"], upload_id_map=self.upload_id_map, prefix=op["key_prefix"])
                    total_p += op["num_connections"]
                elif op["op_type"] == "read_kafka":
                    # THIS BLOCK IS KEPT FROM YOUR 'stream' VERSION
                    operators[handle] = GatewayKafkaReadOperator(
                        handle=handle, region=self.region, input_queue=input_queue, output_queue=output_queue,
                        error_queue=self.error_queue, error_event=self.error_event, chunk_store=self.chunk_store,
                        n_processes=op.get("num_connections", 1),
                        kafka_brokers=op.get("kafka_brokers"),
                        topic=op.get("topic"),
                        group_id=op.get("group_id", f"skyplane-consumer-{handle.replace('/','-').replace(':','_')}")
                        ,
                        target_batch_size_mb=op.get("target_batch_size_mb", 32),
                        max_batch_time_seconds=op.get("max_batch_time_seconds", 0.05),
                        max_messages_per_batch=op.get("max_messages_per_batch", 10000),
                    )
                    total_p += op.get("num_connections", 1)
                elif op["op_type"] == "write_kafka":
                    # THIS BLOCK IS KEPT FROM YOUR 'stream' VERSION
                    n_conn = int(op.get("num_connections", 1))
                    operators[handle] = GatewayKafkaWriteOperator(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_queue=self.error_queue,
                        error_event=self.error_event,
                        chunk_store=self.chunk_store,
                        n_processes=n_conn,
                        kafka_brokers=op.get("kafka_brokers"),
                        topic=op.get("topic"),
                        preserve_partitions=op.get("preserve_partitions", True),
                        acks=op.get("acks", "all"),
                        enable_idempotence=op.get("enable_idempotence", True),
                        max_in_flight_requests_per_connection=op.get("max_in_flight_requests_per_connection", 1),
                        max_message_bytes=op.get("max_message_bytes", 800 * 1024),
                    )
                    total_p += n_conn
                elif op["op_type"] == "write_local":
                    operators[handle] = GatewayWriteLocal(
                        handle=handle, region=self.region, input_queue=input_queue, output_queue=output_queue,
                        error_queue=self.error_queue, error_event=self.error_event, chunk_store=self.chunk_store,
                        n_processes=op.get("num_connections", 1),
                        path=op.get("path"),
                        stats_queue=self.write_stats_queue,
                    )
                    total_p += op.get("num_connections", 1)
                else:
                    raise ValueError(f"Unsupported op_type {op['op_type']}")

                total_p += create_gateway_operators_helper(output_queue, child_operators, partition_ids)

            return total_p

        # The rest of the method is unchanged
        print(gateway_program)
        total_p = 0
        for program_group in gateway_program:
            partitions = program_group["partitions"]
            program = program_group["value"]
            print("partitions", partitions)
            if program[0]["op_type"] == "mux_and":
                queue = GatewayANDQueue()
                print(f"First operator is mux_and: queue {queue}")
                assert len(program) == 1, f"mux_and cannot have siblings"
                program = program[0]["children"]
                for partition in partitions:
                    self.num_required_terminal[str(partition)] = len(program)
            else:
                queue = GatewayQueue(maxsize=5000)
                print(f"First operator is ", program[0], "queue", queue)
                for partition in partitions:
                    self.num_required_terminal[str(partition)] = 1
            for partition in partitions:
                self.chunk_store.add_partition(str(partition), queue)
            total_p += create_gateway_operators_helper(
                self.chunk_store.chunk_requests[str(partitions[0])],
                program,
                partitions,
            )
        return operators

    def run(self):
        # This method is unchanged
        exit_flag = Event()
        def exit_handler(signum, frame):
            logger.warning("[gateway_daemon] Received signal {}. Exiting...".format(signum))
            exit_flag.set()
            for operator in self.operators.values():
                operator.stop_workers()
            sys.exit(0)
        for operator in self.operators.values():
            logger.info(f"[gateway_daemon] Starting gateway operator {operator.handle} workers")
            operator.start_workers()
        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGTERM, exit_handler)
        logger.info("[gateway_daemon] Starting daemon loop")
        try:
            while not exit_flag.is_set() and not self.error_event.is_set():
                self.api_server.pull_chunk_status_queue()
        except Exception as e:
            self.error_queue.put(e)
            self.error_event.set()
            logger.error(f"[gateway_daemon] Exception in daemon loop: {e}")
            logger.exception(e)
        logger.info("[gateway_daemon] Exiting all workers except for API")
        for operator in self.operators.values():
            operator.stop_workers()
        logger.info("[gateway_daemon] Done")


if __name__ == "__main__":
    # This is unchanged
    parser = argparse.ArgumentParser(description="Skyplane Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region")
    parser.add_argument("--chunk-dir", type=Path, default="/tmp/skyplane/chunks", help="Directory to store chunks")
    parser.add_argument("--disable-tls", action="store_true")
    parser.add_argument("--use-compression", action="store_true")
    parser.add_argument("--disable-e2ee", action="store_true")
    args = parser.parse_args()
    os.makedirs(args.chunk_dir, exist_ok=True)
    daemon = GatewayDaemon(
        region=args.region,
        chunk_dir=args.chunk_dir,
        use_tls=not args.disable_tls,
    )
    daemon.run()