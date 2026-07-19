print("<<<<< LOADING MODIFIED gateway_operator.py FILE - VERSION >>>>>")

import json
from pathlib import Path
#import logging # Or from skyplane.utils import logger if that's the project's pattern
#logger_op = logging.getLogger(__name__)
import os
from typing import List
import queue
import socket
import shutil
import ssl
import struct
import io
import time
import lz4.frame
import traceback
import uuid
import threading
import traceback
from kafka import KafkaConsumer
from confluent_kafka import Producer as CfProducer
from kafka.errors import KafkaError
from functools import partial
from multiprocessing import Event, Process, Queue
from multiprocessing.managers import DictProxy
from typing import Dict, List, Optional

import urllib3
import nacl.secret
from abc import ABC, abstractmethod

from skyplane.config_paths import cloud_config
from skyplane.utils.definitions import MB
from skyplane.utils import logger
from skyplane.utils.retry import retry_backoff
from skyplane.utils.timer import Timer
from skyplane.obj_store.object_store_interface import ObjectStoreInterface

from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.gateway.gateway_queue import GatewayQueue
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.gateway.chunk_store import ChunkStore


class GatewayOperator(ABC):
    def __init__(
        self,
        handle: str,
        region: str,  # TODO: remove
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        n_processes: Optional[int] = 1,
    ):
        self.handle = handle
        self.region = region
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.chunk_store = chunk_store
        self.error_event = error_event
        self.error_queue = error_queue
        self.n_processes = n_processes

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # args for worker function
        self.args = ()

        # shared state
        # TODO: move this outside of GateWaySender to share between processes?
        self.processes = []
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: Optional[int] = None

    def start_workers(self):
        for i in range(self.n_processes):
            p = Process(target=self.worker_loop, args=(i,) + self.args)
            p.start()
            self.processes.append(p)

    def stop_workers(self):
        for i in range(self.n_processes):
            self.exit_flags[i].set()
        for p in self.processes:
            p.join()
        self.processes = []

    def worker_loop(self, worker_id: int, *args):
        self.worker_id = worker_id
        while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
            try:
                # get chunk from input queue
                try:
                    # will only get data for that handle
                    chunk_req = self.input_queue.get_nowait(self.handle)
                except queue.Empty:
                    continue

                # TODO: status logging
                self.chunk_store.log_chunk_state(chunk_req, ChunkState.in_progress, operator_handle=self.handle, worker_id=worker_id)
                # process chunk
                succ = self.process(chunk_req, *args)

                # place in output queue
                if succ:
                    self.chunk_store.log_chunk_state(chunk_req, ChunkState.complete, operator_handle=self.handle, worker_id=worker_id)
                    if self.output_queue is not None:
                        self.output_queue.put(chunk_req)
                    else:
                        print(f"[{self.handle}:{self.worker_id}] Output queue is None - terminal operator")
                    time.sleep(0.1)  # yield ?
                else:
                    # failed to process - re-queue
                    time.sleep(0.1)
                    self.input_queue.put(chunk_req)

            except Exception as e:
                logger.error(f"[{self.handle}:{self.worker_id}] Exception: {e}")
                self.error_queue.put(traceback.format_exc())
                self.error_event.set()
                self.exit_flags[worker_id].set()

        # run worker exit function
        self.worker_exit(worker_id)

    def worker_exit(self, worker_id: int):
        pass

    @abstractmethod
    def process(self, chunk_req: ChunkRequest, **args):
        pass


class GatewayWaitReceiver(GatewayOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process(self, chunk_req: ChunkRequest):
        logger.info(f"[{self.handle}:{self.worker_id}] Processing chunk {chunk_req.chunk.chunk_id}. Checking for file.")
        chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)
        if not os.path.exists(chunk_file_path):  # chunk still not downloaded, re-queue
            logger.warning(f"[{self.handle}:{self.worker_id}] Chunk file {chunk_file_path} not found yet. Re-queuing.")
            return False

        # check to see if file is completed downloading
        with open(chunk_file_path, "rb") as f:
            data = f.read()
            if len(data) < chunk_req.chunk.chunk_length_bytes:
                # download not complete
                return False
            assert (
                len(data) == chunk_req.chunk.chunk_length_bytes
            ), f"Downloaded chunk length does not match expected length: {len(data)}, {chunk_req.chunk.chunk_length_bytes}"
        
        logger.info(
            f"[{self.handle}:{self.worker_id}] Successfully received chunk {chunk_req.chunk.chunk_id}, {len(data)}, {chunk_req.chunk.chunk_length_bytes}"
        )
        return True

class GatewaySender(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        ip_addr: str,
        use_tls: Optional[bool] = True,
        use_compression: Optional[bool] = True,
        e2ee_key_bytes: Optional[bytes] = None,
        n_processes: Optional[int] = 32,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        self.ip_addr = ip_addr
        self.use_tls = use_tls
        self.use_compression = use_compression
        self.e2ee_key_bytes = e2ee_key_bytes
        self.args = (ip_addr,)
        
        # --- NEW LOGGER ---
        # This will print once when the operator is created, confirming the new code is loaded.
        logger.info(f"##### GatewaySender INITIALIZED with robust error handling. TLS enabled: {self.use_tls} #####")
        # --- END NEW LOGGER ---
 
        # Per-worker cumulative counters
        self._chunks_sent_total = 0
        self._bytes_sent_raw_total = 0
        self._bytes_wire_sent_total = 0

        # Initialize crypto/TLS and HTTP client up-front (used during process())
        if e2ee_key_bytes:
            self.e2ee_secretbox = nacl.secret.SecretBox(e2ee_key_bytes)
        else:
            self.e2ee_secretbox = None

        if use_tls:
            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE
        else:
            self.ssl_context = None

        self.destination_ports: Dict[str, int] = {}
        self.destination_sockets: Dict[str, socket.socket] = {}

        timeout = urllib3.util.Timeout(connect=10.0, read=None)
        self.http_pool = urllib3.PoolManager(
            retries=urllib3.Retry(total=3, backoff_factor=0.5),
            cert_reqs="CERT_NONE",
            timeout=timeout,
        )
 
    def worker_loop(self, worker_id: int, *args):
        self.worker_id = worker_id
        # Periodic summary logger for net send
        def _summary_loop():
            while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
                logger.info(f"[{self.handle}:{self.worker_id}] Summary[net send]: chunks={self._chunks_sent_total}, raw_bytes={self._bytes_sent_raw_total}, wire_bytes={self._bytes_wire_sent_total}")
                time.sleep(5)
        threading.Thread(target=_summary_loop, daemon=True).start()
        super().worker_loop(worker_id, *args)
 
        # init already done in __init__

    def worker_exit(self, worker_id: int):
        for dst_socket in self.destination_sockets.values():
            dst_socket.close()
        for dst_host, dst_port in self.destination_ports.items():
            protocol = "https" if self.use_tls else "http"
            url = f"{protocol}://{dst_host}:8081/api/v1/servers/{dst_port}"
            try:
                self.http_pool.request("DELETE", url)
            except Exception as e:
                logger.error(f"[{self.handle}:{worker_id}] Exception closing server port {dst_port} on {dst_host}: {e}")

    def make_socket(self, dst_host: str) -> socket.socket:
        protocol = "https" if self.use_tls else "http"
        url = f"{protocol}://{dst_host}:8081/api/v1/servers"
        
        response = self.http_pool.request("POST", url)
        if response.status != 200:
            raise ConnectionRefusedError(f"Failed to get new server port from {url}: {response.status} {response.data.decode()}")

        port = int(json.loads(response.data.decode("utf-8"))["server_port"])
        self.destination_ports[dst_host] = port
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((dst_host, port))
        
        if self.ssl_context:
            sock = self.ssl_context.wrap_socket(sock, server_side=False)

        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock

    def process(self, chunk_req: ChunkRequest, dst_host: str) -> bool:
        chunk_id = chunk_req.chunk.chunk_id
        
        # Step 1: Pre-register the chunk.
        try:
            protocol = "https" if self.use_tls else "http"
            url = f"{protocol}://{dst_host}:8081/api/v1/chunk_requests"
            register_body = json.dumps([chunk_req.as_dict()]).encode("utf-8")
            
            # --- NEW LOGGER ---
            logger.info(f"[sender:{self.worker_id}][{chunk_id}] STEP 1: Attempting to register chunk via API call to {url}")
            # --- END NEW LOGGER ---
            
            response = self.http_pool.request("POST", url, body=register_body, headers={"Content-Type": "application/json"})
            if response.status != 200:
                logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] FAILED REGISTRATION with {dst_host}: {response.status} {response.data.decode('utf-8')}")
                return False
        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] EXCEPTION during registration to {dst_host}: {e}", exc_info=True)
            return False

        # Step 2: Send the chunk data.
        try:
            if dst_host not in self.destination_sockets:
                self.destination_sockets[dst_host] = self.make_socket(dst_host)
            sock = self.destination_sockets[dst_host]

            chunk = chunk_req.chunk
            chunk_file_path = self.chunk_store.get_chunk_file_path(chunk.chunk_id)
            with open(chunk_file_path, "rb") as f:
                data = f.read()

            wire_length, raw_wire_length, is_compressed = len(data), len(data), False
            if self.use_compression:
                data = lz4.frame.compress(data)
                wire_length, is_compressed = len(data), True
            if self.e2ee_secretbox:
                data = self.e2ee_secretbox.encrypt(data)
                wire_length = len(data)

            header = chunk.to_wire_header(
                n_chunks_left_on_socket=0,
                wire_length=wire_length,
                raw_wire_length=raw_wire_length,
                is_compressed=is_compressed,
            )
            
            # --- NEW LOGGER ---
            logger.info(f"[sender:{self.worker_id}][{chunk_id}] STEP 2: Attempting to send {raw_wire_length/MB:.2f}MB of data")
            # --- END NEW LOGGER ---

            header.to_socket(sock)
            with Timer() as t:
                sock.sendall(data)
            logger.info(f"[sender:{self.worker_id}][{chunk.chunk_id}] SUCCESS: sent {raw_wire_length/MB:.2f}MB at {wire_length * 8 / t.elapsed / MB:.2f}Mbps")
            # Update and log counters
            self._chunks_sent_total += 1
            self._bytes_sent_raw_total += raw_wire_length
            self._bytes_wire_sent_total += wire_length
            logger.info(f"[{self.handle}:{self.worker_id}] Counters[net send]: chunks={self._chunks_sent_total}, raw_bytes={self._bytes_sent_raw_total}, wire_bytes={self._bytes_wire_sent_total}")
            
        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}][{chunk.chunk_id}] FAILED SEND, recreating socket: {e}", exc_info=True)
            if dst_host in self.destination_sockets:
                self.destination_sockets[dst_host].close()
                del self.destination_sockets[dst_host]
            return False

        return True

class GatewayRandomDataGen(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        size_mb: int,
        n_processes: Optional[int] = 1,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        self.size_mb = size_mb

    def process(self, chunk_req: ChunkRequest):
        # wait until enough space available
        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
        size_bytes = int(self.size_mb * MB)
        assert size_bytes > 0, f"Invalid size {size_bytes} for fallocate"

        while True:
            # create file with random data
            try:
                os.system(f"fallocate -l {size_bytes} {fpath}")
                file_size = os.path.getsize(fpath)
                if file_size == size_bytes:
                    break
            except Exception:
                print(f"[gen_data] Chunk store full, waiting before generating {chunk_req.chunk.chunk_id}")
                time.sleep(0.1)
                continue

        logger.info(f"[{self.handle}:{self.worker_id}] Wrote chunk {chunk_req.chunk.chunk_id} with size {file_size} to {fpath}")
        chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)

        return True


class GatewayWriteLocal(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        n_processes: int = 1,
        path: Optional[str] = None,
        stats_queue: Optional[Queue] = None,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        if path is None:
            logger.warning(f"[{self.handle}] GatewayWriteLocal: No output path provided in __init__. Will not write files.")
            self.configured_path = None
        else:
            self.configured_path = Path(path)
            try:
                self.configured_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"[{self.handle}] GatewayWriteLocal output path configured to: {self.configured_path.resolve()}")
            except Exception as e:
                logger.error(f"[{self.handle}] Error creating output directory {self.configured_path}: {e}")
                self.configured_path = None

        # Throughput accounting
        self._bytes_written_total = 0
        self._files_written_total = 0
        self._msgs_written_total = 0
        # Start timing from first chunk received by this writer
        self._start_time = None
        self._tp_lock = threading.Lock()
        self._stats_queue = stats_queue

    def _infer_message_count(self, file_path: Path) -> int:
        """
        Best-effort message count by peeking at batch headers:
        - SKMETA01: [8B magic][U32 num_records]
        - Legacy batch: [U32 num_records]
        - Otherwise: treat as 1 message (raw slice)
        """
        try:
            with open(file_path, "rb") as f:
                head = f.read(16)
            magic = getattr(GatewayKafkaReadOperator, '_BATCH_MAGIC', b'SKMETA01')
            if len(head) >= len(magic) + 4 and head[:len(magic)] == magic:
                import struct as _struct
                return int(_struct.unpack("!I", head[len(magic):len(magic)+4])[0])
            if len(head) >= 4:
                import struct as _struct
                return int(_struct.unpack("!I", head[:4])[0])
        except Exception:
            pass
        return 1

    def worker_loop(self, worker_id: int, *args):
        self.worker_id = worker_id
        # Periodic throughput summary
        def _summary_loop():
            while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
                with self._tp_lock:
                    start_time = self._start_time
                    elapsed = max(1e-6, time.time() - start_time) if start_time is not None else 0.0
                    mb = self._bytes_written_total / (1024 * 1024)
                    mbps = (mb / elapsed) if elapsed > 0 else 0.0
                    files = self._files_written_total
                    msgs = self._msgs_written_total
                logger.info(f"[{self.handle}:{self.worker_id}] Summary[local write]: files={files}, msgs={msgs}, bytes={self._bytes_written_total}, avg_throughput={mbps:.2f} MB/s")
                time.sleep(5)
        threading.Thread(target=_summary_loop, daemon=True).start()
        super().worker_loop(worker_id, *args)

    def worker_exit(self, worker_id: int):
        # Print a final one-line summary similar to the source producer
        with self._tp_lock:
            start_time = self._start_time
            elapsed = max(1e-6, time.time() - start_time) if start_time is not None else 0.0
            total_mb = self._bytes_written_total / (1024 * 1024)
            mbps = (total_mb / elapsed) if elapsed > 0 else 0.0
            files = self._files_written_total
            msgs = self._msgs_written_total
        logger.info(
            f"[{self.handle}:{worker_id}] Completed local writes: files={files}, msgs={msgs} ({total_mb:.2f} MB) in {elapsed:.2f}s -> {mbps:.2f} MB/s"
        )

    def process(self, chunk_req: ChunkRequest):
        if not self.configured_path:
            logger.error(f"[{self.handle}:{self.worker_id}] GatewayWriteLocal: No valid output path. Chunk {chunk_req.chunk.chunk_id} write skipped.")
            return True 

        src_chunk_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)
        
        output_filename_str = getattr(chunk_req.chunk, 'dest_key', None)
        if not output_filename_str: 
            output_filename_str = f"{chunk_req.chunk.chunk_id}.default.data"
            logger.warning(f"[{self.handle}:{self.worker_id}] No 'dest_key' on chunk for {chunk_req.chunk.chunk_id}, using default: {output_filename_str}")
        
        output_filename = Path(output_filename_str).name 
        dest_path = self.configured_path / output_filename

        # Snapshot the next file index for logging
        with self._tp_lock:
            next_file_index = self._files_written_total + 1
        logger.info(f"[{self.handle}:{self.worker_id}] Attempting to write chunk {chunk_req.chunk.chunk_id} from {src_chunk_path} to {dest_path} (file_index={next_file_index})")

        try:
            if not src_chunk_path.exists():
                logger.error(f"[{self.handle}:{self.worker_id}] Source file {src_chunk_path} not found for {chunk_req.chunk.chunk_id}")
                return False 
            with Timer() as t:
                with open(src_chunk_path, "rb") as f_src, open(dest_path, "wb") as f_dest:
                    shutil.copyfileobj(f_src, f_dest)
            
            file_size = dest_path.stat().st_size
            # Infer message count for reporting
            msg_count = self._infer_message_count(src_chunk_path)
            logger.info(f"[{self.handle}:{self.worker_id}] Successfully WROTE chunk {chunk_req.chunk.chunk_id} data to {dest_path} (size: {file_size} bytes, msgs={msg_count}, file_index={next_file_index})")
            # Initialize start time at first successful write
            with self._tp_lock:
                if self._start_time is None:
                    self._start_time = time.time()

            # Update throughput counters
            with self._tp_lock:
                self._bytes_written_total += file_size
                self._files_written_total += 1
                self._msgs_written_total += msg_count
                if t.elapsed > 0:
                    inst_mbps = (file_size / (1024 * 1024)) / t.elapsed
                    logger.info(f"[{self.handle}:{self.worker_id}] Instant: msgs={msg_count}, bytes={file_size}, time={t.elapsed:.2f}s -> {inst_mbps:.2f} MB/s")
                # Also log totals so far for convenience
                elapsed = max(1e-6, time.time() - self._start_time) if self._start_time is not None else 0.0
                avg_mbps = ((self._bytes_written_total / (1024 * 1024)) / elapsed) if elapsed > 0 else 0.0
                logger.info(f"[{self.handle}:{self.worker_id}] Totals so far: files={self._files_written_total}, msgs={self._msgs_written_total}, bytes={self._bytes_written_total}, avg_throughput={avg_mbps:.2f} MB/s")
            
            if hasattr(chunk_req.chunk, 'chunk_length_bytes') and \
               chunk_req.chunk.chunk_length_bytes > 0 and \
               file_size != chunk_req.chunk.chunk_length_bytes:
                logger.warning(f"[{self.handle}:{self.worker_id}] Size mismatch for {dest_path}. Expected {chunk_req.chunk.chunk_length_bytes}, got {file_size}")

            return True
        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}] Error writing chunk data {chunk_req.chunk.chunk_id} to {dest_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

class GatewayKafkaReadOperator(GatewayOperator):
    def __init__(self,
                 handle: str,
                 region: str,
                 input_queue: GatewayQueue,
                 output_queue: GatewayQueue,
                 error_event,
                 error_queue: Queue,
                 chunk_store: ChunkStore,
                 n_processes: int = 1,
                 kafka_brokers: str = None,
                 topic: str = None,
                 group_id: str = "skyplane-kafka-reader",
                 # Batching configuration parameters
                 target_batch_size_mb: int = 32,
                 max_batch_time_seconds: float = 1.0,
                 max_messages_per_batch: int = 10000,
                 **kwargs):
        # Honor configured n_processes (parallel readers)
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes=n_processes)
        self.kafka_brokers_str = kafka_brokers
        self.topic_to_consume = topic
        self.group_id = group_id
        self.consumer = None
        
        # Batching configuration
        self.target_batch_size = target_batch_size_mb * 1024 * 1024
        self.max_batch_time_s = max_batch_time_seconds
        self.max_messages_per_batch = max_messages_per_batch

        if not self.kafka_brokers_str or not self.topic_to_consume:
            logger.error(f"[{self.handle}] KafkaReadOperator: Configuration is missing!")

    def process(self, chunk_req: ChunkRequest, **args):
        """
        Main processing loop with robust, race-condition-free batching and a manual commit protocol.
        """
        worker_id = self.worker_id
        
        try:
            logger.info(f"[{self.handle}:{worker_id}] Initializing KafkaConsumer for brokers: {self.kafka_brokers_str}, group: {self.group_id}")
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers_str.split(','),
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # CRITICAL: Manual commits for reliability
                
                # High-performance settings from your advanced version
                fetch_min_bytes=1048576,
                fetch_max_wait_ms=100,
                max_partition_fetch_bytes=64 * 1024 * 1024,
                fetch_max_bytes=1024 * 1024 * 1024,
                max_poll_records=10000,
                receive_buffer_bytes=67108864,
                send_buffer_bytes=2 * 1024 * 1024,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=600000,
                request_timeout_ms=40000,
                check_crcs=False,
                retry_backoff_ms=50,
            )
            self.consumer.subscribe([self.topic_to_consume])
            logger.info(f"[{self.handle}:{worker_id}] KafkaConsumer initialized and subscribed.")
        except Exception as e:
            logger.error(f"[{self.handle}:{worker_id}] FATAL: Failed to initialize KafkaConsumer: {e}", exc_info=True)
            self.error_event.set()
            return False

        # Batching and state variables
        # Each element: {
        #   'key': Optional[bytes], 'value': Optional[bytes], 'timestamp_ms': Optional[int],
        #   'headers': List[Tuple[str, Optional[bytes]]], 'partition': int, 'offset': int
        # }
        batch_messages = []
        batch_size_bytes = 0
        last_send_time = time.time()
        no_new_messages_count = 0
        max_empty_polls = 50  # A safe, high value (500 * 100ms poll timeout = 50s)

        logger.info(f"[{self.handle}:{worker_id}] Starting robust batching loop...")

        while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
            try:
                message_dict = self.consumer.poll(timeout_ms=100, max_records=5000)

                # --- CORRECT, SIMPLE, AND ROBUST BATCHING LOGIC ---

                # 1. Add ALL newly polled messages to the current batch first.
                if message_dict:
                    no_new_messages_count = 0  # Reset counter on new messages
                    for records in message_dict.values():
                        for record in records:
                            try:
                                value_bytes = record.value if record.value is not None else None
                                key_bytes = record.key if record.key is not None else None
                                # List[Tuple[str, Optional[bytes]]]; append source topic for downstream observability
                                headers_list = (record.headers or []) + [
                                    ("src.topic", self.topic_to_consume.encode('utf-8')),
                                ]
                                # Append full record metadata for downstream preservation
                                batch_messages.append({
                                    'key': key_bytes,
                                    'value': value_bytes,
                                    'timestamp_ms': record.timestamp if hasattr(record, 'timestamp') else None,
                                    'headers': headers_list,
                                    'partition': record.partition if hasattr(record, 'partition') else -1,
                                    'offset': record.offset if hasattr(record, 'offset') else -1,
                                })
                                # Use value length as batching heuristic like before (tolerate None)
                                batch_size_bytes += len(value_bytes) if value_bytes is not None else 0
                            except Exception as rec_e:
                                logger.error(f"[{self.handle}:{worker_id}] Error capturing Kafka record metadata: {rec_e}")
                else:
                    no_new_messages_count += 1

                # 2. Check if the CURRENT, fully-populated batch should be sent.
                time_since_last_send = time.time() - last_send_time
                is_size_triggered = batch_size_bytes >= self.target_batch_size
                is_time_triggered = batch_messages and time_since_last_send >= self.max_batch_time_s
                is_final_batch = batch_messages and no_new_messages_count >= max_empty_polls
                
                # 3. If any trigger is met, send the ENTIRE current batch and THEN commit.
                if is_size_triggered or is_time_triggered or is_final_batch:
                    reason = "size" if is_size_triggered else "time" if is_time_triggered else "final"
                    logger.info(f"[{self.handle}:{worker_id}] Sending batch due to {reason} trigger ({len(batch_messages)} msgs)...")
                    
                    if self.send_batch(batch_messages, chunk_req):
                        # The key to reliability: commit everything consumed so far, but only AFTER a successful send.
                        self.consumer.commit()
                        logger.info(f"[{self.handle}:{worker_id}] ✅ Commit successful.")
                        
                        # Reset for the next batch
                        batch_messages, batch_size_bytes = [], 0
                        last_send_time = time.time()
                    else:
                         logger.error(f"[{self.handle}:{worker_id}] Send batch failed. Will retry processing on next loop.")
                         # We don't reset or commit, so the failed batch will be re-processed.

            except Exception as e:
                logger.error(f"[{self.handle}:{worker_id}] Error in polling loop: {e}", exc_info=True)
                self.error_event.set()
                break
        
        # Final flush on clean exit (e.g., Ctrl+C)
        if batch_messages:
            logger.info(f"[{self.handle}:{worker_id}] Flushing final batch on exit...")
            if self.send_batch(batch_messages, chunk_req):
                self.consumer.commit()
                logger.info(f"[{self.handle}:{worker_id}] ✅ Committed final offsets.")

        if self.consumer:
            self.consumer.close()
        logger.info(f"[{self.handle}:{worker_id}] Kafka consumer loop exited.")
        return True

    # Magic prefix for metadata-aware batch frames: b"SKMETA01"
    _BATCH_MAGIC = b"SKMETA01"

    def send_batch(self, batch_messages: List[dict], trigger_chunk_req: ChunkRequest) -> bool:
        """Serialize a metadata-aware batch frame and enqueue downstream. Returns True on success.

        Frame layout (big-endian):
          [8B magic 'SKMETA01']
          [U32 num_records]
          For each record i in 1..N:
            [U32 key_len (0xFFFFFFFF for None)]
            [U32 value_len (0xFFFFFFFF for None)]
            [I64 timestamp_ms (-1 if unknown)]
            [I32 src_partition]
            [I64 src_offset]
            [U32 header_count]
            [U32 hkey_len][hkey_utf8_bytes][U32 hval_len (0xFFFFFFFF for None)][hval_bytes?] repeated header_count
            [key_bytes?]
            [value_bytes?]
        """
        if not self.output_queue:
            logger.warning(f"[{self.handle}:{self.worker_id}] No output queue, discarding batch.")
            return False
        
        if not batch_messages:
            logger.warning(f"[{self.handle}:{self.worker_id}] Attempted to send empty batch.")
            return True # Not a failure, just nothing to send

        new_chunk_id = uuid.uuid4().hex
        
        try:
            parts = [self._BATCH_MAGIC, struct.pack("!I", len(batch_messages))]
            # First encode fixed-length metadata and headers, then payloads
            payload_parts = []
            for rec in batch_messages:
                key_bytes = rec.get('key', None)
                val_bytes = rec.get('value', None)
                ts_ms = rec.get('timestamp_ms', None)
                part = rec.get('partition', -1)
                off = rec.get('offset', -1)
                headers = rec.get('headers', []) or []

                key_len = 0xFFFFFFFF if key_bytes is None else len(key_bytes)
                val_len = 0xFFFFFFFF if val_bytes is None else len(val_bytes)
                ts_field = -1 if ts_ms is None else int(ts_ms)
                parts.append(struct.pack("!IIqiQI", key_len, val_len, ts_field, int(part), int(off), len(headers)))

                # Headers: ensure names are str, values Optional[bytes]
                for hname, hval in headers:
                    try:
                        hname_b = hname.encode('utf-8') if isinstance(hname, str) else bytes(hname)
                    except Exception:
                        hname_b = str(hname).encode('utf-8')
                    hval_len = 0xFFFFFFFF if hval is None else len(hval)
                    parts.append(struct.pack("!I", len(hname_b)))
                    parts.append(hname_b)
                    parts.append(struct.pack("!I", hval_len))
                    if hval is not None:
                        parts.append(hval)

                # Defer appending key/value bytes to avoid interleaving with metadata fields
                if key_bytes is not None:
                    payload_parts.append(key_bytes)
                if val_bytes is not None:
                    payload_parts.append(val_bytes)

            batch_data = b"".join(parts + payload_parts)

            fpath = self.chunk_store.get_chunk_file_path(new_chunk_id)
            with open(fpath, "wb") as f:
                f.write(batch_data)

            new_chunk = Chunk(
                chunk_id=new_chunk_id,
                src_key=trigger_chunk_req.chunk.src_key,
                dest_key=trigger_chunk_req.chunk.dest_key,
                chunk_length_bytes=len(batch_data),
                partition_id=trigger_chunk_req.chunk.partition_id
            )
            new_req_dict = vars(trigger_chunk_req).copy()
            new_req_dict['chunk'] = new_chunk
            new_req = ChunkRequest(**new_req_dict)
            
            logger.info(f"[{self.handle}:{self.worker_id}] Sending batch {new_chunk_id} downstream ({len(batch_messages)} msgs, {len(batch_data)/MB:.2f}MB).")
            self.output_queue.put(new_req)
            return True
            
        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}] Error in send_batch: {e}", exc_info=True)
            return False
            
class GatewayKafkaWriteOperator(GatewayOperator):
    def __init__(self,
                 handle: str,
                 region: str,
                 input_queue: GatewayQueue,
                 output_queue: GatewayQueue,
                 error_event,
                 error_queue: Queue,
                 chunk_store: ChunkStore,
                 n_processes: int = 4,
                 kafka_brokers: str = None,
                 topic: str = None,
                 max_message_bytes: int = 800 * 1024,
                 preserve_partitions: bool = True,
                 acks: str = "1",
                 enable_idempotence: bool = False,
                 max_in_flight_requests_per_connection: int = 5,
                 linger_ms: int = 100,
                 minimal_headers: bool = False,
                 **kwargs): # Use kwargs
        
        # Pass only parent arguments
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        
        # Save child-specific arguments
        self.kafka_brokers_str = kafka_brokers
        self.topic_to_produce = topic
        self.producer = None
        self.max_message_bytes = max_message_bytes
        self.preserve_partitions = preserve_partitions
        # Producer tuning overrides
        self.producer_acks = acks
        self.producer_enable_idempotence = enable_idempotence
        self.producer_max_in_flight = max_in_flight_requests_per_connection
        self._dst_partitions = set()
        self._produced_messages = 0
        self._planned_messages = 0
        # Thread-safe ack/fail counters (per process)
        self._acked_messages = 0
        self._failed_messages = 0
        self._ack_lock = threading.Lock()
        # Bytes counters
        self._planned_bytes_total = 0
        self._produced_bytes_total = 0
        # Chunks fully completed (all messages for the chunk flushed)
        self._completed_chunks = 0

        # Round-robin partitioning state (per process)
        self._dst_partitions_list = []
        self._rr_index = 0
        self.producer_linger_ms = int(linger_ms)
        self.minimal_headers = bool(minimal_headers)

        if not self.kafka_brokers_str or not self.topic_to_produce:
            logger.error(f"[{self.handle}] KafkaWriteOperator: configuration is missing!")
    
    def _on_ack(self, record_metadata):
        with self._ack_lock:
            self._acked_messages += 1
    
    def _on_err(self, exc):
        with self._ack_lock:
            self._failed_messages += 1
        logger.error(f"[{self.handle}:{self.worker_id}] Kafka send error: {exc}")

    def worker_loop(self, worker_id: int, *args):
        self.worker_id = worker_id
        if self.kafka_brokers_str:
            try:
                logger.info(f"[{self.handle}:{self.worker_id}] Initializing HIGH-PERFORMANCE KafkaProducer for brokers: {self.kafka_brokers_str}")
                # Coerce acks to the exact type expected by kafka-python
                acks_value = self.producer_acks
                if isinstance(acks_value, str):
                    if acks_value.strip() == "1":
                        acks_value = 1
                    elif acks_value.strip() == "0":
                        acks_value = 0
                    elif acks_value.strip().lower() == "all":
                        acks_value = "all"
                # Idempotence requires max_in_flight <= 5 – enforce defensively
                if self.producer_enable_idempotence and self.producer_max_in_flight > 5:
                    logger.warning(f"[{self.handle}:{self.worker_id}] Reducing max_in_flight from {self.producer_max_in_flight} to 5 for idempotent producer safety")
                    self.producer_max_in_flight = 5
                logger.info(f"[{self.handle}:{self.worker_id}] Producer config: acks={acks_value}, idempotence={self.producer_enable_idempotence}, max_in_flight={self.producer_max_in_flight}")
                # Map to confluent_kafka (librdkafka) configuration
                if isinstance(acks_value, int):
                    acks_str = str(acks_value)
                else:
                    acks_str = str(acks_value)
                cf_conf = {
                    "bootstrap.servers": self.kafka_brokers_str,
                    "acks": acks_str,
                    "compression.type": "none",
                    "linger.ms": int(self.producer_linger_ms),
                    "batch.num.messages": 100000,
                    "queue.buffering.max.kbytes": 1048576,   # 512 MB
                    "queue.buffering.max.messages": 1000000,
                    "message.max.bytes": 134217728,
                    "max.in.flight.requests.per.connection": int(self.producer_max_in_flight),
                    "retries": 3,
                    "enable.idempotence": bool(self.producer_enable_idempotence),
                    "socket.keepalive.enable": True,
                    "request.timeout.ms": 120000,
                    "message.timeout.ms": 300000,
                }
                self.producer = CfProducer(cf_conf)
                logger.info(f"[{self.handle}:{self.worker_id}] HIGH-PERFORMANCE KafkaProducer initialized.")
                try:
                    md = self.producer.list_topics(self.topic_to_produce, timeout=10.0)
                    tmd = md.topics.get(self.topic_to_produce)
                    if tmd is not None and not getattr(tmd, 'error', None):
                        parts = set(tmd.partitions.keys())
                    else:
                        parts = set()
                    self._dst_partitions = set(parts)
                    self._dst_partitions_list = sorted(list(self._dst_partitions))
                    logger.info(f"[{self.handle}:{self.worker_id}] Destination topic '{self.topic_to_produce}' partitions: {sorted(self._dst_partitions)}")
                except Exception as e:
                    logger.warning(f"[{self.handle}:{self.worker_id}] Unable to fetch partitions for topic '{self.topic_to_produce}': {e}")
            except Exception as e:
                logger.error(f"[{self.handle}:{self.worker_id}] FATAL: Failed to initialize KafkaProducer: {e}")
                self.producer = None
        else:
            logger.error(f"[{self.handle}:{self.worker_id}] FATAL: Kafka brokers not configured.")
            self.producer = None

        # Periodic summary logger for Kafka write
        def _summary_loop():
            while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
                with self._ack_lock:
                    acked = self._acked_messages
                    failed = self._failed_messages
                logger.info(
                    f"[{self.handle}:{self.worker_id}] Summary[Kafka write]: planned_msgs={self._planned_messages}, planned_bytes={self._planned_bytes_total}, produced_msgs={self._produced_messages}, produced_bytes={self._produced_bytes_total}, acked={acked}, failed={failed}, completed_chunks={self._completed_chunks}"
                )
                time.sleep(5)
        threading.Thread(target=_summary_loop, daemon=True).start()

        super().worker_loop(worker_id, *args)

        if self.producer:
            logger.info(f"[{self.handle}:{self.worker_id}] Flushing and closing KafkaProducer.")
            self.producer.flush()
            self.producer.close()
            logger.info(f"[{self.handle}:{self.worker_id}] Total produced messages: {self._produced_messages}")

    def process(self, chunk_req: ChunkRequest):
        if not self.producer: return False
        
        src_chunk_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)

        try:
            import time as _time
            with open(src_chunk_path, "rb") as f:
                file_bytes = f.read()

            # Try to parse as metadata-aware batch first; fallback to legacy length-prefixed; else treat as raw payload
            def _try_parse_metadata_batch(data: bytes):
                magic = getattr(GatewayKafkaReadOperator, '_BATCH_MAGIC', b'SKMETA01')
                if len(data) < len(magic) + 4 or data[:len(magic)] != magic:
                    return None
                pos = len(magic)
                try:
                    num = struct.unpack("!I", data[pos:pos+4])[0]
                    pos += 4
                    records = []
                    for _ in range(num):
                        if pos + 4*2 + 8 + 4 + 8 + 4 > len(data):
                            return None
                        key_len, val_len, ts_ms, src_part, src_off, hcount = struct.unpack("!IIqiQI", data[pos:pos+4+4+8+4+8+4])
                        pos += (4+4+8+4+8+4)
                        headers = []
                        for _h in range(hcount):
                            if pos + 4 > len(data):
                                return None
                            name_len = struct.unpack("!I", data[pos:pos+4])[0]
                            pos += 4
                            if pos + name_len > len(data):
                                return None
                            name_b = data[pos:pos+name_len]
                            pos += name_len
                            if pos + 4 > len(data):
                                return None
                            hval_len = struct.unpack("!I", data[pos:pos+4])[0]
                            pos += 4
                            if hval_len == 0xFFFFFFFF:
                                hval = None
                            else:
                                if pos + hval_len > len(data):
                                    return None
                                hval = data[pos:pos+hval_len]
                                pos += hval_len
                            headers.append((name_b.decode('utf-8'), hval))

                        # key/value bytes are appended at the tail in sequence of messages
                        # We cannot know their positions without scanning all headers; hence we appended them after metadata.
                        records.append({
                            'key_len': key_len,
                            'val_len': val_len,
                            'timestamp_ms': ts_ms,
                            'src_partition': src_part,
                            'src_offset': src_off,
                            'headers': headers,
                        })

                    # After reading all metadata, the remaining bytes are concatenated key/value payloads in order of messages
                    payload_pos = pos
                    for rec in records:
                        if rec['key_len'] != 0xFFFFFFFF:
                            klen = rec['key_len']
                            if payload_pos + klen > len(data):
                                return None
                            rec['key'] = data[payload_pos:payload_pos+klen]
                            payload_pos += klen
                        else:
                            rec['key'] = None
                        if rec['val_len'] != 0xFFFFFFFF:
                            vlen = rec['val_len']
                            if payload_pos + vlen > len(data):
                                return None
                            rec['value'] = data[payload_pos:payload_pos+vlen]
                            payload_pos += vlen
                        else:
                            rec['value'] = None

                    return records
                except Exception:
                    return None

            def _try_parse_batch(data: bytes):
                if len(data) < 4:
                    return None
                try:
                    stream = io.BytesIO(data)
                    num_msgs = struct.unpack("!I", stream.read(4))[0]
                    msgs = []
                    total = 4
                    for _ in range(num_msgs):
                        if total + 4 > len(data):
                            return None
                        mlen = struct.unpack("!I", stream.read(4))[0]
                        total += 4
                        if total + mlen > len(data):
                            return None
                        msgs.append(stream.read(mlen))
                        total += mlen
                    if total != len(data):
                        return None
                    return msgs
                except Exception:
                    return None

            meta_records = _try_parse_metadata_batch(file_bytes)
            if meta_records is not None:
                # Metadata-aware batch: preserve key, headers, and timestamp_ms
                planned_cnt = len(meta_records)
                planned_bytes = sum((len(rec.get('value') or b"")) for rec in meta_records)
                self._planned_messages += planned_cnt
                self._planned_bytes_total += planned_bytes
                logger.info(f"[{self.handle}:{self.worker_id}] Planning to produce {planned_cnt} messages (metadata batch) for chunk {chunk_req.chunk.chunk_id}. Planned total: {self._planned_messages}")
                cnt = 0
                prev_acked = self._acked_messages
                prev_failed = self._failed_messages
                for i, rec in enumerate(meta_records):
                    headers = [] if self.minimal_headers else list(rec.get('headers', []))
                    # Attach source partition/offset as informational headers (cannot preserve offsets)
                    if not self.minimal_headers:
                        headers.append(("src.partition", str(rec.get('src_partition', -1)).encode('utf-8')))
                        headers.append(("src.offset", str(rec.get('src_offset', -1)).encode('utf-8')))
                    # Optionally preserve partition number when destination supports it
                    partition_arg = None
                    src_part = rec.get('src_partition', -1)
                    if self.preserve_partitions and isinstance(src_part, int) and src_part >= 0 and src_part in self._dst_partitions:
                        partition_arg = src_part
                    # Drop message key to allow partitioner to distribute when not preserving partitions
                    key_to_send = rec.get('key') if self.preserve_partitions else None
                    # Explicitly spread across partitions when not preserving
                    if not self.preserve_partitions and self._dst_partitions_list:
                        partition_arg = self._dst_partitions_list[self._rr_index % len(self._dst_partitions_list)]
                        self._rr_index = (self._rr_index + 1) % max(1, len(self._dst_partitions_list))
                    def _cb(err, _msg):
                        if err: self._on_err(err)
                        else: self._on_ack(None)
                    try:
                        _timestamp = (None if rec.get('timestamp_ms', -1) == -1 else int(rec.get('timestamp_ms')))
                        produce_kwargs = {
                            "topic": self.topic_to_produce,
                            "key": key_to_send,
                            "value": rec.get('value'),
                            "headers": headers,
                            "on_delivery": _cb,
                        }
                        if partition_arg is not None:
                            produce_kwargs["partition"] = int(partition_arg)
                        if _timestamp is not None:
                            produce_kwargs["timestamp"] = _timestamp
                        self.producer.produce(**produce_kwargs)
                    except BufferError:
                        self.producer.poll(0.01)
                        _timestamp = (None if rec.get('timestamp_ms', -1) == -1 else int(rec.get('timestamp_ms')))
                        produce_kwargs = {
                            "topic": self.topic_to_produce,
                            "key": key_to_send,
                            "value": rec.get('value'),
                            "headers": headers,
                            "on_delivery": _cb,
                        }
                        if partition_arg is not None:
                            produce_kwargs["partition"] = int(partition_arg)
                        if _timestamp is not None:
                            produce_kwargs["timestamp"] = _timestamp
                        self.producer.produce(**produce_kwargs)
                    cnt += 1
                    self.producer.poll(0)
                self._produced_messages += cnt
                self._produced_bytes_total += planned_bytes
                acked_since = self._acked_messages - prev_acked
                failed_since = self._failed_messages - prev_failed
                logger.info(f"[{self.handle}:{self.worker_id}] Acked {acked_since}/{cnt}, failed {failed_since} for chunk {chunk_req.chunk.chunk_id}. Total acked: {self._acked_messages}")
                logger.info(f"[{self.handle}:{self.worker_id}] Flushed {cnt} messages ({planned_bytes} bytes) for chunk {chunk_req.chunk.chunk_id}. Totals[Kafka write]: produced_msgs={self._produced_messages}, produced_bytes={self._produced_bytes_total}, planned_msgs={self._planned_messages}, planned_bytes={self._planned_bytes_total}, completed_chunks={self._completed_chunks}")
                return True

            messages = _try_parse_batch(file_bytes)
            if messages is not None:
                # Legacy batch (values only): attach minimal headers per record
                planned_cnt = len(messages)
                planned_bytes = sum(len(m) for m in messages)
                self._planned_messages += planned_cnt
                self._planned_bytes_total += planned_bytes
                logger.info(f"[{self.handle}:{self.worker_id}] Planning to produce {planned_cnt} messages (legacy batch) for chunk {chunk_req.chunk.chunk_id}. Planned total: {self._planned_messages}")
                cnt = 0
                prev_acked = self._acked_messages
                prev_failed = self._failed_messages
                src_bucket = chunk_req.src_object_store_bucket or ""
                src_key = chunk_req.chunk.src_key or ""
                content_type = chunk_req.chunk.mime_type or ""
                batch_seq = chunk_req.chunk.part_number or 0
                key_bytes = None if not self.preserve_partitions else (src_key.encode("utf-8") if src_key else None)
                for i, msg in enumerate(messages):
                    if self.minimal_headers:
                        headers = []
                    else:
                        headers = [
                            ("s3.bucket", src_bucket.encode("utf-8") if src_bucket else b""),
                            ("s3.key", src_key.encode("utf-8") if src_key else b""),
                            ("content.type", content_type.encode("utf-8") if content_type else b""),
                            ("batch.seq", str(batch_seq).encode("utf-8")),
                            ("record.index", str(i).encode("utf-8")),
                        ]
                    partition_arg = None
                    if not self.preserve_partitions and self._dst_partitions_list:
                        partition_arg = self._dst_partitions_list[self._rr_index % len(self._dst_partitions_list)]
                        self._rr_index = (self._rr_index + 1) % max(1, len(self._dst_partitions_list))
                    def _cb(err, _msg):
                        if err: self._on_err(err)
                        else: self._on_ack(None)
                    try:
                        produce_kwargs = {
                            "topic": self.topic_to_produce,
                            "key": key_bytes,
                            "value": msg,
                            "headers": headers,
                            "on_delivery": _cb,
                        }
                        if partition_arg is not None:
                            produce_kwargs["partition"] = int(partition_arg)
                        self.producer.produce(**produce_kwargs)
                    except BufferError:
                        self.producer.poll(0.01)
                        produce_kwargs = {
                            "topic": self.topic_to_produce,
                            "key": key_bytes,
                            "value": msg,
                            "headers": headers,
                            "on_delivery": _cb,
                        }
                        if partition_arg is not None:
                            produce_kwargs["partition"] = int(partition_arg)
                        self.producer.produce(**produce_kwargs)
                    cnt += 1
                    self.producer.poll(0)
                self._produced_messages += cnt
                self._produced_bytes_total += planned_bytes
                acked_since = self._acked_messages - prev_acked
                failed_since = self._failed_messages - prev_failed
                logger.info(f"[{self.handle}:{self.worker_id}] Acked {acked_since}/{cnt}, failed {failed_since} for chunk {chunk_req.chunk.chunk_id}. Total acked: {self._acked_messages}")
                logger.info(f"[{self.handle}:{self.worker_id}] Flushed {cnt} messages ({planned_bytes} bytes) for chunk {chunk_req.chunk.chunk_id}. Totals[Kafka write]: produced_msgs={self._produced_messages}, produced_bytes={self._produced_bytes_total}, planned_msgs={self._planned_messages}, planned_bytes={self._planned_bytes_total}, completed_chunks={self._completed_chunks}")
                return True

            # Raw chunk mode: split payload to respect max_message_bytes and attach headers
            src_bucket = chunk_req.src_object_store_bucket or ""
            src_key = chunk_req.chunk.src_key or ""
            base_offset = chunk_req.chunk.file_offset_bytes or 0
            part_number = chunk_req.chunk.part_number or 0
            md5 = chunk_req.chunk.md5_hash or b""
            ts_ms = int(_time.time() * 1000)

            slice_size = max(1, self.max_message_bytes)
            total_len = len(file_bytes)
            # Pre-log planned message count before sending
            planned_cnt = (total_len + slice_size - 1) // slice_size
            planned_bytes = total_len
            self._planned_messages += planned_cnt
            self._planned_bytes_total += planned_bytes
            logger.info(f"[{self.handle}:{self.worker_id}] Planning to produce {planned_cnt} messages (raw) for chunk {chunk_req.chunk.chunk_id} (size={total_len}B, slice={slice_size}B). Planned total: {self._planned_messages}")
            sent = 0
            key_bytes = None if not self.preserve_partitions else (src_key.encode("utf-8") if src_key else None)
            pos = 0
            prev_acked = self._acked_messages
            prev_failed = self._failed_messages
            while pos < total_len:
                end = min(pos + slice_size, total_len)
                payload = file_bytes[pos:end]
                headers = [] if self.minimal_headers else [
                    ("chunk_id", chunk_req.chunk.chunk_id.encode("utf-8") if getattr(chunk_req, "chunk", None) and getattr(chunk_req.chunk, "chunk_id", None) else b""),
                    ("bucket", src_bucket.encode("utf-8") if src_bucket else b""),
                    ("key", src_key.encode("utf-8") if src_key else b""),
                    ("offset", str(base_offset + pos).encode("utf-8")),
                    ("length", str(len(payload)).encode("utf-8")),
                    ("part_number", str(part_number).encode("utf-8")),
                    ("md5", md5 if isinstance(md5, (bytes, bytearray)) else b""),
                    ("timestamp_ms", str(ts_ms).encode("utf-8")),
                ]
                partition_arg = None
                if not self.preserve_partitions and self._dst_partitions_list:
                    partition_arg = self._dst_partitions_list[self._rr_index % len(self._dst_partitions_list)]
                    self._rr_index = (self._rr_index + 1) % max(1, len(self._dst_partitions_list))
                def _cb(err, _msg):
                    if err: self._on_err(err)
                    else: self._on_ack(None)
                try:
                    produce_kwargs = {
                        "topic": self.topic_to_produce,
                        "key": key_bytes,
                        "value": payload,
                        "headers": headers,
                        "on_delivery": _cb,
                    }
                    if partition_arg is not None:
                        produce_kwargs["partition"] = int(partition_arg)
                    self.producer.produce(**produce_kwargs)
                except BufferError:
                    self.producer.poll(0.01)
                    produce_kwargs = {
                        "topic": self.topic_to_produce,
                        "key": key_bytes,
                        "value": payload,
                        "headers": headers,
                        "on_delivery": _cb,
                    }
                    if partition_arg is not None:
                        produce_kwargs["partition"] = int(partition_arg)
                    self.producer.produce(**produce_kwargs)
                sent += 1
                pos = end
                self.producer.poll(0)

            self._produced_messages += sent
            self._produced_bytes_total += total_len
            acked_since = self._acked_messages - prev_acked
            failed_since = self._failed_messages - prev_failed
            logger.info(f"[{self.handle}:{self.worker_id}] Acked {acked_since}/{sent}, failed {failed_since} for chunk {chunk_req.chunk.chunk_id}. Total acked: {self._acked_messages}")
            self._completed_chunks += 1
            logger.info(f"[{self.handle}:{self.worker_id}] Flushed {sent} messages ({total_len} bytes, raw mode) for chunk {chunk_req.chunk.chunk_id}. Totals[Kafka write]: produced_msgs={self._produced_messages}, produced_bytes={self._produced_bytes_total}, planned_msgs={self._planned_messages}, planned_bytes={self._planned_bytes_total}, completed_chunks={self._completed_chunks}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}] Exception producing chunk {chunk_req.chunk.chunk_id}: {e}")
            logger.error(traceback.format_exc())
            return False


class GatewayObjStoreOperator(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        bucket_name: str,
        bucket_region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        n_processes: Optional[int] = 1,
        chunk_store: Optional[ChunkStore] = None,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.src_requester_pays = cloud_config.get_flag("requester_pays")

        # process-local state
        self.worker_id: Optional[int] = None
        self.obj_store_interfaces: Dict[str, ObjectStoreInterface] = {}

    # interact with object store
    def get_obj_store_interface(self, region: str, bucket: str) -> ObjectStoreInterface:
        key = f"{region}:{bucket}"
        if key not in self.obj_store_interfaces:
            logger.warning(f"[gateway_daemon] ObjectStoreInterface not cached for {key}")
            try:
                self.obj_store_interfaces[key] = ObjectStoreInterface.create(region, bucket)
            except Exception as e:
                raise ValueError(f"Failed to create obj store interface {str(e)}")
        return self.obj_store_interfaces[key]


class GatewayObjStoreReadOperator(GatewayObjStoreOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        bucket_name: str,
        bucket_region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        n_processes: int = 32,
        chunk_store: Optional[ChunkStore] = None,
        csv_target_batch_bytes: Optional[int] = None,
        csv_max_records_per_batch: Optional[int] = None,
        csv_minimal_framing: bool = False,
    ):
        super().__init__(
            handle, region, bucket_name, bucket_region, input_queue, output_queue, error_event, error_queue, n_processes, chunk_store
        )
        # Per-worker cumulative counters
        self._chunks_read_total = 0
        self._bytes_read_total = 0

        # CSV format-aware defaults (kept simple; auto-detected by .csv extension)
        self._csv_enabled = True
        # When True, emit lightweight SKMETA01 frames with zero per-record headers and no key
        # (value-only). Removes the per-record header construction cost on the source hot path,
        # matching Confluent StringFormat's value-only output for fair high-throughput comparison.
        self._csv_minimal_framing = bool(csv_minimal_framing)
        # Allow overrides from program/CLI; fall back to defaults
        self._csv_target_batch_bytes = (
            int(csv_target_batch_bytes) if csv_target_batch_bytes is not None else 32 * 1024 * 1024
        )
        self._csv_max_records_per_batch = (
            int(csv_max_records_per_batch) if csv_max_records_per_batch is not None else 500_000
        )

    def worker_loop(self, worker_id: int, *args):
        self.worker_id = worker_id
        # Periodic summary logger for S3 read
        def _summary_loop():
            while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
                logger.info(f"[{self.handle}:{self.worker_id}] Summary[S3 read]: chunks={self._chunks_read_total}, bytes={self._bytes_read_total}")
                time.sleep(5)
        threading.Thread(target=_summary_loop, daemon=True).start()
        super().worker_loop(worker_id, *args)

    def process(self, chunk_req: ChunkRequest, **args):
        # CSV format-aware fast path: if the object looks like CSV, stream it and emit record-safe batches
        try:
            src_key_lower = (chunk_req.chunk.src_key or "").lower()
        except Exception:
            src_key_lower = ""
        if self._csv_enabled and src_key_lower.endswith(".csv"):
            # Only allow CSV batching on the first chunk (offset 0). Subsequent slice requests become no-ops.
            first_chunk = (chunk_req.chunk.file_offset_bytes or 0) == 0
            if first_chunk:
                return self._process_csv_batches(chunk_req)
            else:
                empty_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)
                Path(str(empty_path)).touch()
                chunk_req.chunk.chunk_length_bytes = 0
                self._chunks_read_total += 1
                logger.info(f"[{self.handle}:{self.worker_id}] CSV mode (no-op) for subsequent chunk: {chunk_req.chunk.src_key} offset={chunk_req.chunk.file_offset_bytes}")
                return True

        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
        # wait for free space
        logger.debug(f"[{self.handle}:{self.worker_id}] Start download {chunk_req.chunk.chunk_id} from {self.bucket_name}")

        obj_store_interface = self.get_obj_store_interface(self.bucket_region, self.bucket_name)

        if self.src_requester_pays:
            obj_store_interface.set_requester_bool(True)

        # while self.chunk_store.remaining_bytes() < chunk_req.chunk.chunk_length_bytes * self.n_processes:
        #    time.sleep(0.1)

        # assert chunk_req.chunk.chunk_length_bytes > 0, f"Cannot have size 0 chunk {chunk_req.chunk}" # actually ok

        if chunk_req.chunk.chunk_length_bytes == 0:
            # nothing to do
            # create empty file
            Path(fpath).touch()
            # Update counters for empty chunk as well
            self._chunks_read_total += 1
            logger.info(f"[{self.handle}:{self.worker_id}] Counters[S3 read]: chunks={self._chunks_read_total}, bytes={self._bytes_read_total}")
            return True

        while True:
            # if self.chunk_store.remaining_bytes() < chunk_req.chunk.chunk_length_bytes * self.n_processes:
            #    time.sleep(0.1)
            #    continue
            try:
                md5sum = retry_backoff(
                    partial(
                        obj_store_interface.download_object,
                        chunk_req.chunk.src_key,
                        fpath,
                        chunk_req.chunk.file_offset_bytes,
                        chunk_req.chunk.chunk_length_bytes,
                        generate_md5=True,
                    ),
                    max_retries=1,  # TODO: fix this - not a good solution
                )

                # ensure properly downloaded
                file_size = os.path.getsize(fpath)
                if file_size == chunk_req.chunk.chunk_length_bytes:
                    break

            except Exception as e:
                logger.error(f"[obj_store:{self.worker_id}] Error reading key {chunk_req.chunk.src_key}: {str(e)}")
                print(f"[obj_store:{self.worker_id}] Error reading key {chunk_req.chunk.src_key}: {str(e)}")
                time.sleep(1)

        # update md5sum for chunk requests
        # TODO: create checksum operator
        # if not md5sum:
        #    logger.error(f"[obj_store:{self.worker_id}] Checksum was not generated for {chunk_req.chunk.src_key}")
        # else:
        #    self.chunk_store.update_chunk_checksum(chunk_req.chunk.chunk_id, md5sum)

        received_chunk_size = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).stat().st_size
        assert (
            received_chunk_size == chunk_req.chunk.chunk_length_bytes
        ), f"Downloaded chunk {chunk_req.chunk.chunk_id} to {fpath} has incorrect size (expected {chunk_req.chunk.chunk_length_bytes} but got {received_chunk_size}, {chunk_req.chunk.chunk_length_bytes})"
        logger.debug(f"[obj_store:{self.worker_id}] Downloaded {chunk_req.chunk.chunk_id} from {self.bucket_name}")

        # Update and log counters
        self._chunks_read_total += 1
        self._bytes_read_total += received_chunk_size
        logger.info(f"[{self.handle}:{self.worker_id}] Counters[S3 read]: chunks={self._chunks_read_total}, bytes={self._bytes_read_total}")
        return True

    def _process_csv_batches(self, chunk_req: ChunkRequest) -> bool:
        """
        CSV record-aware batching with rich metadata frames (SKMETA01):
        - Download the full CSV object to a local temp file
        - Build metadata-aware batches preserving record boundaries and offsets
        - Each record carries Kafka headers: s3.* info, csv.* indices, batch info, and EOF markers
        - Batch frame format matches GatewayKafkaReadOperator._BATCH_MAGIC (SKMETA01)
        """
        try:
            import hashlib

            obj_store_interface = self.get_obj_store_interface(self.bucket_region, self.bucket_name)
            tmp_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)

            logger.info(f"[{self.handle}:{self.worker_id}] CSV mode: downloading full object {chunk_req.chunk.src_key} for record-aware batching")
            _md5 = retry_backoff(
                partial(
                    obj_store_interface.download_object,
                    chunk_req.chunk.src_key,
                    str(tmp_path),
                    None,
                    None,
                    generate_md5=False,
                ),
                max_retries=2,
            )

            batch_seq = 0
            total_bytes_emitted = 0
            total_records_emitted = 0
            global_record_index = 0
            bucket = chunk_req.src_object_store_bucket or ""
            s3_key = chunk_req.chunk.src_key or ""

            # Header detection (first non-empty line treated as header)
            header_bytes: Optional[bytes] = None
            header_sha256_hex: Optional[str] = None

            def build_meta_frame(records: List[bytes], start_idx: int, is_final: bool) -> bytes:
                """Build SKMETA01 metadata-aware frame for the given records."""
                magic = getattr(GatewayKafkaReadOperator, '_BATCH_MAGIC', b'SKMETA01')
                parts = [magic, struct.pack("!I", len(records))]
                payload_parts = []
                batch_start = start_idx
                batch_end = start_idx + len(records) - 1 if records else start_idx - 1
                minimal = self._csv_minimal_framing
                for i, value_bytes in enumerate(records):
                    # key used for partition determinism when preserve_partitions=True;
                    # dropped in minimal framing to match Confluent value-only output.
                    key_bytes = None if minimal else (s3_key.encode('utf-8') if s3_key else None)
                    key_len = 0xFFFFFFFF if key_bytes is None else len(key_bytes)
                    val_len = 0xFFFFFFFF if value_bytes is None else len(value_bytes)
                    ts_field = -1  # unknown timestamp (signed 64-bit)
                    src_part = -1  # unknown partition (signed 32-bit)
                    # src_off is encoded as unsigned 64-bit; use all-ones sentinel for unknown
                    src_off = 0xFFFFFFFFFFFFFFFF
                    # Per-record headers (skipped entirely in minimal framing to avoid the
                    # per-record str()/encode()/struct.pack hot-path cost over millions of records)
                    if minimal:
                        hdrs: List[tuple[str, Optional[bytes]]] = []
                    else:
                        rec_index = batch_start + i
                        hdrs = [
                            ("s3.bucket", bucket.encode('utf-8') if bucket else b""),
                            ("s3.key", s3_key.encode('utf-8') if s3_key else b""),
                            ("csv.header.present", b"1" if header_bytes is not None else b"0"),
                            ("csv.header.sha256", header_sha256_hex.encode('utf-8') if header_sha256_hex else b""),
                            ("csv.batch.seq", str(batch_seq).encode('utf-8')),
                            ("csv.batch.start_record", str(batch_start).encode('utf-8')),
                            ("csv.batch.end_record", str(batch_end).encode('utf-8')),
                            ("csv.record.index", str(rec_index).encode('utf-8')),
                            ("csv.eof", b"1" if (is_final and i == len(records)-1) else b"0"),
                            ("content.type", b"text/csv"),
                        ]
                    parts.append(struct.pack("!IIqiQI", key_len, val_len, ts_field, int(src_part), int(src_off), len(hdrs)))
                    for hname, hval in hdrs:
                        hname_b = hname.encode('utf-8')
                        parts.append(struct.pack("!I", len(hname_b)))
                        parts.append(hname_b)
                        if hval is None:
                            parts.append(struct.pack("!I", 0xFFFFFFFF))
                        else:
                            parts.append(struct.pack("!I", len(hval)))
                            parts.append(hval)
                    if key_bytes is not None:
                        payload_parts.append(key_bytes)
                    if value_bytes is not None:
                        payload_parts.append(value_bytes)
                return b"".join(parts + payload_parts)

            def flush_batch_meta(records: List[bytes], start_idx: int, is_final: bool) -> bool:
                nonlocal batch_seq, total_bytes_emitted, total_records_emitted
                if not records:
                    return True
                frame = build_meta_frame(records, start_idx, is_final)
                new_chunk_id = uuid.uuid4().hex
                dst_path = self.chunk_store.get_chunk_file_path(new_chunk_id)
                with open(dst_path, "wb") as f:
                    f.write(frame)
                new_chunk = Chunk(
                    chunk_id=new_chunk_id,
                    src_key=chunk_req.chunk.src_key,
                    dest_key=chunk_req.chunk.dest_key,
                    chunk_length_bytes=len(frame),
                    partition_id=chunk_req.chunk.partition_id,
                    mime_type="text/csv",
                    part_number=batch_seq,
                )
                new_req_dict = vars(chunk_req).copy()
                new_req_dict['chunk'] = new_chunk
                new_req = ChunkRequest(**new_req_dict)
                logger.info(f"[{self.handle}:{self.worker_id}] CSV(meta) batch seq={batch_seq} records={len(records)} size={len(frame)/MB:.3f}MB final={is_final} -> {new_chunk_id}")
                self.output_queue.put(new_req)
                batch_seq += 1
                total_bytes_emitted += len(frame)
                total_records_emitted += len(records)
                return True

            # Iterate CSV lines, detect header, and build metadata-aware batches
            batch_records: List[bytes] = []
            batch_bytes = 0  # we don't include metadata overhead; just payload approx
            target_bytes = self._csv_target_batch_bytes
            max_records = self._csv_max_records_per_batch
            batch_start_index = 0

            with open(tmp_path, "rb") as f:
                # Header detection: first non-empty line is header
                while True:
                    pos = f.tell()
                    raw = f.readline()
                    if raw == b"":
                        break
                    line = raw.rstrip(b"\n\r")
                    if not line:
                        continue
                    header_bytes = line
                    header_sha256_hex = hashlib.sha256(header_bytes).hexdigest()
                    logger.info(f"[{self.handle}:{self.worker_id}] Detected CSV header ({len(header_bytes)} bytes) for {s3_key}")
                    break
                # Process remaining lines as records
                for raw_line in f:
                    line = raw_line.rstrip(b"\n\r")
                    if not line:
                        continue
                    rec_len = len(line)
                    # If adding would exceed size target, flush current batch (count-first semantics still honored by max_records)
                    if batch_records and (batch_bytes + rec_len) > target_bytes:
                        if not flush_batch_meta(batch_records, batch_start_index, is_final=False):
                            return False
                        batch_records = []
                        batch_bytes = 0
                        batch_start_index = global_record_index
                    batch_records.append(line)
                    batch_bytes += rec_len
                    global_record_index += 1
                    if len(batch_records) >= max_records:
                        if not flush_batch_meta(batch_records, batch_start_index, is_final=False):
                            return False
                        batch_records = []
                        batch_bytes = 0
                        batch_start_index = global_record_index

            # Flush tail as final batch
            if batch_records:
                if not flush_batch_meta(batch_records, batch_start_index, is_final=True):
                    return False

            # Update counters and log
            self._chunks_read_total += 1
            self._bytes_read_total += total_bytes_emitted
            logger.info(f"[{self.handle}:{self.worker_id}] CSV(meta) batching complete for {chunk_req.chunk.src_key}: batches={batch_seq}, records={total_records_emitted}, bytes={total_bytes_emitted}")
            # Truncate original trigger file so downstream treats it as no-op
            try:
                with open(tmp_path, "wb") as _f:
                    pass
                chunk_req.chunk.chunk_length_bytes = 0
            except Exception:
                pass
            return True
        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}] CSV(meta) batching failed for {chunk_req.chunk.src_key}: {e}")
            logger.error(traceback.format_exc())
            return False


class GatewayObjStoreWriteOperator(GatewayObjStoreOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        bucket_name: str,
        bucket_region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        upload_id_map: DictProxy,
        n_processes: Optional[int] = 32,
        chunk_store: Optional[ChunkStore] = None,
        prefix: Optional[str] = "",
    ):
        super().__init__(
            handle, region, bucket_name, bucket_region, input_queue, output_queue, error_event, error_queue, n_processes, chunk_store
        )
        self.chunk_store = chunk_store
        self.upload_id_map = upload_id_map
        self.prefix = prefix

    def process(self, chunk_req: ChunkRequest):
        chunk_id = chunk_req.chunk.chunk_id
        src_key = chunk_req.chunk.src_key
        dest_key = self.prefix + chunk_req.chunk.dest_key

        try:
            fpath_obj = self.chunk_store.get_chunk_file_path(chunk_id)
            fpath = str(fpath_obj.absolute())
            
            if not os.path.exists(fpath):
                logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] CRITICAL: Chunk file {fpath} does not exist. This should not happen after GatewayWaitReceiver. Re-queuing.")
                return False

            obj_store_interface = self.get_obj_store_interface(self.bucket_region, self.bucket_name)
            upload_id = None

            if chunk_req.chunk.multi_part:
                # Canonical key format: "<bucket>:<dest_key>"
                upload_id_key = f"{self.bucket_name}:{dest_key}"
                
                logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Looking for upload ID with key: '{upload_id_key}'")
                logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Available keys in upload_id_map: {list(self.upload_id_map.keys())}")
                
                upload_id = None
                for attempt in range(12):  # Poll for up to 6 seconds
                    upload_id = self.upload_id_map.get(upload_id_key)
                    if upload_id:
                        logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Found upload_id '{upload_id}' for key '{upload_id_key}' on attempt {attempt + 1}")
                        break
                    
                    if attempt == 0:
                        logger.warning(f"[{self.handle}:{self.worker_id}][{chunk_id}] Upload ID for '{upload_id_key}' not yet available. Will retry...")
                    elif attempt % 3 == 0:  # Log every 3rd attempt
                        logger.warning(f"[{self.handle}:{self.worker_id}][{chunk_id}] Still waiting for upload ID '{upload_id_key}' (attempt {attempt + 1}/12)")
                    
                    time.sleep(0.5)

                if not upload_id:
                    logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] FATAL: Could not find upload ID for '{upload_id_key}' after 12 attempts.")
                    logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] Available keys: {list(self.upload_id_map.keys())}")
                    logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] Expected format: '{upload_id_key}' ('<bucket>:<dest_key>')")
                    
                    return False
            
            # ADDITIONAL LOGGING for debugging
            if chunk_req.chunk.multi_part:
                logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Uploading multipart chunk - part {chunk_req.chunk.part_number} with upload_id '{upload_id}'")
            else:
                logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Uploading single-part chunk")

            # Perform the upload with enhanced error handling
            try:
                retry_backoff(
                    partial(
                        obj_store_interface.upload_object,
                        fpath,
                        dest_key,
                        chunk_req.chunk.part_number,
                        upload_id,
                        check_md5=chunk_req.chunk.md5_hash,
                    ),
                    max_retries=5,  # Increased retries for upload robustness
                )
            except Exception as upload_error:
                logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] Upload failed for key '{dest_key}': {upload_error}")
                return False
            
            if chunk_req.chunk.multi_part:
                logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Successfully uploaded multipart chunk - part {chunk_req.chunk.part_number} for key '{dest_key}'")
            else:
                logger.info(f"[{self.handle}:{self.worker_id}][{chunk_id}] Successfully uploaded single-part chunk for key '{dest_key}'")
            
            return True

        except Exception as e:
            logger.error(f"[{self.handle}:{self.worker_id}][{chunk_id}] Exception during upload process for key '{dest_key}': {e}", exc_info=True)
            return False