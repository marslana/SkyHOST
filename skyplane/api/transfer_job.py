import json
import time
import typer
import math
import queue
import sys
import threading
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from functools import partial
from queue import Queue
from typing import TYPE_CHECKING, Callable, Dict, Generator, List, Optional, Tuple, TypeVar
import urllib3
from rich import print as rprint

from skyplane import exceptions
from skyplane.api.config import TransferConfig
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.obj_store.storage_interface import StorageInterface
from skyplane.utils import logger
from skyplane.utils.definitions import MB
from skyplane.utils.fn import do_parallel
from skyplane.utils.path import parse_path
from skyplane.utils.retry import retry_backoff

if TYPE_CHECKING:
    from skyplane.api.dataplane import Dataplane

T = TypeVar("T")

@dataclass
class TransferPair:
    def __init__(self, src_obj: ObjectStoreObject, dst_objs: Dict[str, ObjectStoreObject], dst_key: str):
        self.src_obj, self.dst_objs, self.dst_key = src_obj, dst_objs, dst_key

@dataclass
class GatewayMessage:
    def __init__(self, chunk: Optional[Chunk] = None, upload_id_mapping: Optional[Dict[str, Tuple[str, str, str]]] = None):
        self.chunk, self.upload_id_mapping = chunk, upload_id_mapping

class Chunker:
    def __init__(self, src_iface: StorageInterface, dst_ifaces: List[StorageInterface], transfer_config: TransferConfig, concurrent_multipart_chunk_threads: int = 64, num_partitions: int = 1):
        self.src_iface, self.dst_ifaces, self.transfer_config = src_iface, dst_ifaces, transfer_config
        self.multipart_upload_requests, self.concurrent_multipart_chunk_threads, self.num_partitions = [], concurrent_multipart_chunk_threads, num_partitions
        

    def _run_multipart_chunk_thread(self, exit_event: threading.Event, in_queue: "Queue[TransferPair]", out_queue_chunks: "Queue[GatewayMessage]"):
        while not exit_event.is_set():
            try:
                transfer_pair = in_queue.get(block=False, timeout=0.1)
            except queue.Empty: continue
            src_object, dest_objects, dest_key = transfer_pair.src_obj, transfer_pair.dst_objs, transfer_pair.dst_key
            if isinstance(self.src_iface, ObjectStoreInterface):
                mime_type = self.src_iface.get_obj_mime_type(src_object.key)
                upload_id_mapping = {
                    iface.region_tag(): (
                        iface.bucket(),
                        dest_objects[iface.region_tag()].key,  # destination object key (may include prefix)
                        iface.initiate_multipart_upload(dest_objects[iface.region_tag()].key, mime_type=mime_type),
                    )
                    for iface in self.dst_ifaces
                }
                out_queue_chunks.put(GatewayMessage(upload_id_mapping=upload_id_mapping))
                chunk_size_bytes = int(self.transfer_config.multipart_chunk_size_mb * MB)
                num_chunks = math.ceil(src_object.size / chunk_size_bytes)
                if num_chunks > self.transfer_config.multipart_max_chunks:
                    chunk_size_bytes = math.ceil(src_object.size / self.transfer_config.multipart_max_chunks / MB) * MB
                    num_chunks = math.ceil(src_object.size / chunk_size_bytes)
                offset, part_num, parts = 0, 1, []
                for _ in range(num_chunks):
                    file_size_bytes = min(chunk_size_bytes, src_object.size - offset)
                    chunk = Chunk(src_key=src_object.key, dest_key=dest_key, chunk_id=uuid.uuid4().hex, file_offset_bytes=offset, partition_id=str(part_num % self.num_partitions), chunk_length_bytes=file_size_bytes, part_number=part_num, multi_part=True)
                    offset += file_size_bytes
                    parts.append(part_num)
                    part_num += 1
                    out_queue_chunks.put(GatewayMessage(chunk=chunk))
                for iface in self.dst_ifaces:
                    _, _, upload_id = upload_id_mapping[iface.region_tag()]
                    metadata = None
                    if iface.provider == "azure":
                        from skyplane.obj_store.azure_blob_interface import AzureBlobInterface
                        metadata = (list(map(lambda p: AzureBlobInterface.id_to_base64_encoding(p, dest_objects[iface.region_tag()].key), parts)), mime_type)
                    self.multipart_upload_requests.append(dict(upload_id=upload_id, key=dest_objects[iface.region_tag()].key, parts=parts, region=iface.region_tag(), bucket=iface.bucket(), metadata=metadata))
            else:
                raise NotImplementedError("Multipart not supported for non-object store interfaces")

    @staticmethod
    def map_object_key_prefix(source_prefix: str, source_key: str, dest_prefix: str, recursive: bool = False):
        """This is the robust key mapping logic from your working transfer_job_cp.py"""
        join = lambda prefix, fname: prefix + fname if prefix.endswith("/") else prefix + "/" + fname
        src_fname = source_key.split("/")[-1] if "/" in source_key and not source_key.endswith("/") else source_key
        if not recursive:
            if source_key == source_prefix:
                if dest_prefix == "" or dest_prefix == "/": return src_fname
                elif dest_prefix.endswith("/"): return dest_prefix + src_fname
                else: return dest_prefix
            else:
                rprint(f"\n:x: [bold red]To transfer a prefix, use --recursive (-r). To transfer a single object, provide the full key.[/bold red]")
                raise exceptions.MissingObjectException("Encountered a recursive transfer without the --recursive flag.") from None
        else:
            if source_prefix == "" or source_prefix == "/":
                if dest_prefix == "" or dest_prefix == "/": return source_key
                else: return join(dest_prefix, source_key)
            else:
                if source_key == source_prefix:
                     if dest_prefix.endswith("/"): return join(dest_prefix, src_fname)
                     else: return dest_prefix
                if not source_key.startswith(source_prefix + ("/" if not source_prefix.endswith("/") else "")):
                    raise exceptions.MissingObjectException(f"Source key '{source_key}' does not start with source prefix '{source_prefix}'")
                relative_path = source_key[len(source_prefix):].lstrip("/")
                return join(dest_prefix, relative_path)

    def transfer_pair_generator(self, src_prefix: str, dst_prefixes: List[str], recursive: bool, prefilter_fn: Optional[Callable[[ObjectStoreObject], bool]] = None) -> Generator[TransferPair, None, None]:
        if not isinstance(self.src_iface, ObjectStoreInterface) or not all(isinstance(d, ObjectStoreInterface) for d in self.dst_ifaces):
            raise NotImplementedError("TransferPair generation only supports object store interfaces")
        for iface in self.dst_ifaces:
            if not iface.bucket_exists(): raise exceptions.MissingBucketException(f"Destination bucket {iface.path()} does not exist.")
        if not self.src_iface.bucket_exists(): raise exceptions.MissingBucketException(f"Source bucket {self.src_iface.path()} does not exist.")
        n_objs = 0
        for obj in self.src_iface.list_objects(src_prefix):
            if prefilter_fn is None or prefilter_fn(obj):
                dest_objs, dest_keys = {}, []
                for i, dst_iface in enumerate(self.dst_ifaces):
                    dst_prefix, dest_provider = dst_prefixes[i], dst_iface.provider
                    dest_key = self.map_object_key_prefix(src_prefix, obj.key, dst_prefix, recursive=recursive)
                    dest_keys.append(dest_key[len(dst_prefix) if dst_prefix else 0 :])
                    provider_map = {"aws": "s3_interface", "azure": "azure_blob_interface", "gcp": "gcs_interface", "cloudflare": "r2_interface", "scp": "scp_interface"}
                    obj_class_map = {"aws": "S3Object", "azure": "AzureBlobObject", "gcp": "GCSObject", "cloudflare": "R2Object", "scp": "SCPObject"}
                    if dest_provider in provider_map:
                        mod = __import__(f"skyplane.obj_store.{provider_map[dest_provider]}", fromlist=[obj_class_map[dest_provider]])
                        dest_obj = getattr(mod, obj_class_map[dest_provider])(dest_key, dest_provider, dst_iface.bucket())
                        dest_objs[dst_iface.region_tag()] = dest_obj
                    else: raise ValueError(f"Unsupported provider: {dest_provider}")
                assert len(set(dest_keys)) == 1, f"Keys do not match: {dest_keys}"
                n_objs += 1
                yield TransferPair(src_obj=obj, dst_objs=dest_objs, dst_key=dest_keys[0])
        if n_objs == 0:
            try:
                if not (isinstance(self.src_iface, ObjectStoreInterface) and self.src_iface.get_obj_size(src_prefix) is not None):
                    raise exceptions.MissingObjectException()
            except exceptions.MissingObjectException:
                raise exceptions.MissingObjectException(f"No objects found at source: {self.src_iface.path(src_prefix)}")

    def chunk(self, transfer_pair_generator: Generator[TransferPair, None, None]) -> Generator[GatewayMessage, None, None]:
        multipart_send_queue, multipart_chunk_queue, exit_event = Queue(), Queue(), threading.Event()
        multipart_threads: List[threading.Thread] = []
        if self.transfer_config.multipart_enabled:
            for _ in range(self.concurrent_multipart_chunk_threads):
                t = threading.Thread(target=self._run_multipart_chunk_thread, args=(exit_event, multipart_send_queue, multipart_chunk_queue), daemon=True)
                t.start()
                multipart_threads.append(t)
        for pair in transfer_pair_generator:
            if self.transfer_config.multipart_enabled and pair.src_obj.size > self.transfer_config.multipart_threshold_mb * MB:
                multipart_send_queue.put(pair)
            else:
                if pair.src_obj.size > 0:
                    yield GatewayMessage(chunk=Chunk(src_key=pair.src_obj.key, dest_key=pair.dst_key, chunk_id=uuid.uuid4().hex, chunk_length_bytes=pair.src_obj.size, partition_id="0"))
            while not multipart_chunk_queue.empty():
                yield multipart_chunk_queue.get()
        if self.transfer_config.multipart_enabled:
            while not multipart_send_queue.empty(): time.sleep(0.1)
            exit_event.set()
            for t in multipart_threads: t.join()
            while not multipart_chunk_queue.empty(): yield multipart_chunk_queue.get()

    @staticmethod
    def batch_generator(gen_in: Generator[T, None, None], batch_size: int) -> Generator[List[T], None, None]:
        batch = []
        for item in gen_in:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def prefetch_generator(gen_in: Generator[T, None, None], buffer_size: int) -> Generator[T, None, None]:
        sentinel, queue_buffer = object(), Queue(maxsize=buffer_size)
        def prefetch():
            for item in gen_in: queue_buffer.put(item)
            queue_buffer.put(sentinel)
        thread = threading.Thread(target=prefetch, daemon=True)
        thread.start()
        while True:
            item = queue_buffer.get()
            if item is sentinel: break
            yield item

    @staticmethod
    def tail_generator(gen_in: Generator[T, None, None], out_list: List[T]) -> Generator[T, None, None]:
        for item in gen_in:
            out_list.append(item)
            yield item


@dataclass
class TransferJob(ABC):
    def __init__(self, src_path: str, dst_paths: List[str], recursive: bool = False, requester_pays: bool = False, job_id: Optional[str] = None):
        self.src_path, self.dst_paths = src_path, dst_paths if isinstance(dst_paths, list) else [dst_paths]
        self.recursive, self.requester_pays = recursive, requester_pays
        self.uuid = job_id or str(uuid.uuid4())
    
    @property
    def src_prefix(self) -> str:
        if not hasattr(self, "_src_prefix"): self._src_prefix = parse_path(self.src_path)[2]
        return self._src_prefix

    @property
    def dst_prefixes(self) -> List[str]:
        if not hasattr(self, "_dst_prefixes"): self._dst_prefixes = [parse_path(p)[2] for p in self.dst_paths]
        return self._dst_prefixes

    @property
    def src_iface(self) -> StorageInterface:
        if not hasattr(self, "_src_iface"):
            provider, bucket, _ = parse_path(self.src_path)
            self._src_iface = StorageInterface.create(f"{provider}:infer", bucket)
            if self.requester_pays: self._src_iface.set_requester_bool(True)
        return self._src_iface

    @property
    def dst_ifaces(self) -> List[StorageInterface]:
        if not hasattr(self, "_dst_ifaces"): self._dst_ifaces = [StorageInterface.create(f"{p}:infer", b) for p, b, _ in (parse_path(path) for path in self.dst_paths)]
        return self._dst_ifaces
    
    @abstractmethod
    def dispatch(self, dataplane: "Dataplane", **kwargs) -> Generator[Chunk, None, None]: raise NotImplementedError
    def finalize(self): raise NotImplementedError
    def verify(self): raise NotImplementedError
    def size_gb(self): raise NotImplementedError
    @classmethod
    def _pre_filter_fn(cls, obj: ObjectStoreObject) -> bool: return True


@dataclass
class CopyJob(TransferJob):
    def __init__(self, src_path: str, dst_paths: List[str], recursive: bool = False, requester_pays: bool = False, job_id: Optional[str] = None):
        super().__init__(src_path, dst_paths, recursive, requester_pays, job_id)
        self.transfer_list: List[TransferPair] = []
        self.multipart_transfer_list: List[Dict] = []

    @property
    def http_pool(self):
        if not hasattr(self, "_http_pool"):
            self._http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), timeout=urllib3.util.Timeout(connect=10.0, read=None))
        return self._http_pool

    def gen_transfer_pairs(self, chunker: Optional[Chunker] = None, transfer_config: Optional[TransferConfig] = None) -> Generator[TransferPair, None, None]:
        chunker = chunker or Chunker(self.src_iface, self.dst_ifaces, transfer_config or TransferConfig())
        yield from chunker.transfer_pair_generator(self.src_prefix, self.dst_prefixes, self.recursive, self._pre_filter_fn)

    def dispatch(self, dataplane: "Dataplane", dispatch_batch_size: int = 100, transfer_config: Optional[TransferConfig] = None) -> Generator[Chunk, None, None]:
        if transfer_config is None:
            transfer_config = TransferConfig()

        chunker = Chunker(self.src_iface, self.dst_ifaces, transfer_config)
        pair_generator = self.gen_transfer_pairs(chunker, transfer_config)
        tailed_pairs = chunker.tail_generator(pair_generator, self.transfer_list)
        chunk_generator = chunker.chunk(tailed_pairs)
        batches = chunker.batch_generator(chunker.prefetch_generator(chunk_generator, buffer_size=dispatch_batch_size * 32), batch_size=dispatch_batch_size)

        src_gateways = dataplane.source_gateways()
        queue_size = [0] * len(src_gateways)
        n_multiparts = 0

        def mapping_request(gateway, mappings):
            return self.http_pool.request("POST", f"{gateway.gateway_api_url}/api/v1/upload_id_maps", body=json.dumps(mappings).encode(), headers={"Content-Type": "application/json"})

        def chunk_request(server, requests_batch):
            body = json.dumps([req.as_dict() for req in requests_batch])
            return self.http_pool.request("POST", f"{server.gateway_api_url}/api/v1/chunk_requests", body=body.encode(), headers={"Content-Type": "application/json"})

        # Track which upload IDs have been sent to avoid duplicate sends
        sent_upload_ids = set()

        for batch in batches:
            # 1. FIRST: Send upload_id mappings to DESTINATION gateways and WAIT for confirmation
            upload_id_batch = [msg for msg in batch if msg.upload_id_mapping is not None]
            
            if upload_id_batch:
                sink_gateways = dataplane.sink_gateways()
                
                # Prepare mappings by region using a canonical key: "<bucket>:<dest_key>"
                all_mappings = defaultdict(lambda: defaultdict(dict))
                for msg in upload_id_batch:
                    logger.info(f"CRITICAL DEBUG: msg.upload_id_mapping = {msg.upload_id_mapping}")
                    for region, (bucket, dest_key, upload_id) in msg.upload_id_mapping.items():
                        upload_key = f"{bucket}:{dest_key}"
                        logger.info(f"DEBUG DISPATCH: region='{region}', bucket='{bucket}', dest_key='{dest_key}', upload_key='{upload_key}'")
                        if upload_key not in sent_upload_ids:
                            all_mappings[region][upload_key] = upload_id
                            sent_upload_ids.add(upload_key)
                
                # Send to ALL destination gateways and wait for confirmation
                for region, gateways in sink_gateways.items():
                    if region in all_mappings and all_mappings[region]:
                        logger.info(f"Sending upload ID mappings to region {region}: {list(all_mappings[region].keys())}")
                        
                        # Send to ALL gateways in the region
                        for i, gateway in enumerate(gateways):
                            mappings_payload = dict(all_mappings[region])
                            
                            logger.info(f"Sending payload to gateway {i+1}/{len(gateways)} - {gateway.instance_name()}: {mappings_payload}")
                            
                            try:
                                reply = retry_backoff(partial(mapping_request, gateway, {region: mappings_payload}), max_retries=5)
                                if reply.status != 200:
                                    logger.error(f"Failed to update upload IDs on {gateway.instance_name()}: {reply.data.decode()}")
                                    raise Exception(f"Failed to update upload IDs on {gateway.instance_name()}: {reply.data.decode()}")
                                logger.info(f"SUCCESS: Sent upload ID mappings to gateway {i+1}/{len(gateways)} - {gateway.instance_name()}")
                            except Exception as e:
                                logger.error(f"Exception sending upload IDs to {gateway.instance_name()}: {e}")
                                raise
                        
                        # Allow time for processing across gateways
                        logger.info(f"Waiting 3 seconds for upload IDs to be processed across all {len(gateways)} gateways...")
                        time.sleep(3.0)

            # 2. SECOND: Send ChunkRequests to SOURCE gateways (after upload IDs are confirmed)
            chunk_batch = [msg.chunk for msg in batch if msg.chunk is not None]
            if not chunk_batch: 
                continue
            
            # Create proper ChunkRequest objects
            chunk_requests_to_send = [
                ChunkRequest(
                    chunk=chunk, 
                    src_region=self.src_iface.region_tag(), 
                    dst_region=self.dst_ifaces[0].region_tag(), 
                    src_object_store_bucket=self.src_iface.bucket(), 
                    dst_object_store_bucket=self.dst_ifaces[0].bucket()
                )
                for chunk in chunk_batch
            ]
            
            # Set partition ID to job UUID
            for req in chunk_requests_to_send:
                req.chunk.partition_id = self.uuid

            # Send to SOURCE gateways only
            n_added = 0
            while n_added < len(chunk_requests_to_send):
                if not src_gateways: 
                    raise Exception("No source gateways available to dispatch chunks.")
                
                # Round-robin across source gateways based on queue size
                min_idx = min(range(len(src_gateways)), key=queue_size.__getitem__)
                server = src_gateways[min_idx]
                
                # Send chunk requests to source gateway
                reply = retry_backoff(partial(chunk_request, server, chunk_requests_to_send[n_added:]), max_retries=3)
                if reply.status != 200: 
                    raise Exception(f"Failed to dispatch chunks to {server.instance_name()}: {reply.data.decode()}")
                
                reply_json = json.loads(reply.data.decode())
                n_added += reply_json["n_added"]
                queue_size[min_idx] = reply_json["qsize"]

            # Yield chunks for tracking
            yield from chunk_batch
            
            # Update multipart transfer list
            updated_len = len(chunker.multipart_upload_requests)
            self.multipart_transfer_list.extend(chunker.multipart_upload_requests[n_multiparts:])
            n_multiparts = updated_len


    # 2. Fix the finalize method to properly complete multipart uploads
    def finalize(self):
        if not self.multipart_transfer_list: 
            return
            
        typer.secho("Finalizing multipart uploads...", fg="bright_black")
        groups = defaultdict(list)
        
        # Group by region and bucket
        for req in self.multipart_transfer_list: 
            groups[(req["region"], req["bucket"])].append(req)
        
        def complete_fn(batch):
            if not batch: 
                return
                
            region, bucket = batch[0]["region"], batch[0]["bucket"]
            iface = StorageInterface.create(region, bucket)
            
            # Complete each multipart upload
            for req in batch: 
                try:
                    # CRITICAL FIX: Add proper error handling and metadata
                    metadata = req.get("metadata")
                    parts = req.get("parts", [])
                    
                    # Ensure parts are properly formatted
                    if not parts:
                        logger.error(f"No parts found for multipart upload {req['upload_id']}")
                        continue
                    
                    logger.info(f"Completing multipart upload for {req['key']} with {len(parts)} parts")
                    
                    retry_backoff(
                        partial(iface.complete_multipart_upload, req["key"], req["upload_id"], metadata), 
                        max_retries=5  # Increased retries for completion
                    )
                    
                    logger.info(f"Successfully completed multipart upload for {req['key']}")
                    
                except Exception as e:
                    logger.error(f"Failed to complete multipart upload for {req['key']}: {e}")
                    # Don't re-raise to allow other uploads to complete
                    continue
        
        # Process completions in smaller batches to avoid overwhelming the API
        all_batches = [
            group[i:i+16]  # Smaller batch size for stability
            for group in groups.values() 
            for i in range(0, len(group), 16)
        ]
        
        if all_batches: 
            do_parallel(complete_fn, all_batches, n=4)  # Reduced parallelism
        
        self.multipart_transfer_list.clear()


    # 3. Fix the verification logic to wait for object completion
    def verify(self):
        def verify_region(i):
            dst_iface, dst_prefix = self.dst_ifaces[i], self.dst_prefixes[i]
            dst_keys = {pair.dst_objs[dst_iface.region_tag()].key: pair.src_obj for pair in self.transfer_list}
            
            # CRITICAL FIX: Add retry logic for verification to handle eventual consistency
            max_verify_retries = 3
            
            for retry in range(max_verify_retries):
                failed_keys = {}
                
                for obj in dst_iface.list_objects(dst_prefix):
                    src_obj = dst_keys.get(obj.key)
                    if src_obj:
                        # Check size and modification time
                        if src_obj.size == obj.size and src_obj.last_modified <= obj.last_modified:
                            # Object verified successfully, remove from check list
                            if obj.key in dst_keys:
                                del dst_keys[obj.key]
                        else:
                            failed_keys[obj.key] = f"Size mismatch: expected {src_obj.size}, got {obj.size}"
                
                # If no objects failed verification, we're done
                if not dst_keys and not failed_keys:
                    logger.info(f"Verification successful for region {dst_iface.region_tag()}")
                    return
                
                # If this is not the last retry, wait before retrying
                if retry < max_verify_retries - 1:
                    wait_time = 2 ** retry  # Exponential backoff
                    logger.warning(f"Verification retry {retry + 1}/{max_verify_retries} in {wait_time}s for region {dst_iface.region_tag()}")
                    time.sleep(wait_time)
            
            # If we get here, verification failed after all retries
            if dst_keys or failed_keys:
                error_msg = f"Destination {dst_iface.path()}: {len(dst_keys)} objects missing"
                if failed_keys:
                    error_msg += f", {len(failed_keys)} objects failed size/time check"
                logger.error(error_msg)
                raise exceptions.TransferFailedException(error_msg)
        
        do_parallel(verify_region, range(len(self.dst_ifaces)), n=1, desc="Verifying objects")

    def size_gb(self):
        return sum(p.src_obj.size for p in self.gen_transfer_pairs()) / 1e9


@dataclass
class SyncJob(CopyJob):
    def __init__(self, src_path: str, dst_paths: List[str], requester_pays: bool = False, job_id: Optional[str] = None):
        super().__init__(src_path, dst_paths, True, requester_pays, job_id)
        if not isinstance(self.src_iface, ObjectStoreInterface) or any(not isinstance(iface, ObjectStoreInterface) for iface in self.dst_ifaces):
            raise NotImplementedError("Sync only supports object store interfaces")
    def gen_transfer_pairs(self, chunker: Optional[Chunker] = None, transfer_config: Optional[TransferConfig] = None) -> Generator[TransferPair, None, None]:
        chunker = chunker or Chunker(self.src_iface, self.dst_ifaces, transfer_config or TransferConfig())
        pair_gen = chunker.transfer_pair_generator(self.src_prefix, self.dst_prefixes, self.recursive, self._pre_filter_fn)
        assert len(self.dst_ifaces) == 1, "Sync only supports a single destination"
        for src_obj, dest_obj in self._enrich_dest_objs(pair_gen):
            if self._post_filter_fn(src_obj, dest_obj):
                yield TransferPair(src_obj=src_obj, dst_objs={self.dst_ifaces[0].region_tag(): dest_obj}, dst_key=dest_obj.key.replace(self.dst_prefixes[0], "", 1))
    def _enrich_dest_objs(self, transfer_pairs: Generator[TransferPair, None, None]) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        dst_iface, dest_prefix = self.dst_ifaces[0], self.dst_prefixes[0]
        found_dest_objs = {obj.key: obj for obj in dst_iface.list_objects(dest_prefix)}
        for pair in transfer_pairs:
            src_obj, dest_obj = pair.src_obj, list(pair.dst_objs.values())[0]
            if dest_obj.key in found_dest_objs:
                dest_obj.size, dest_obj.last_modified = found_dest_objs[dest_obj.key].size, found_dest_objs[dest_obj.key].last_modified
            yield src_obj, dest_obj
    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        return not dest_obj.exists or src_obj.last_modified > dest_obj.last_modified or src_obj.size != dest_obj.size


@dataclass
class StreamJob(TransferJob):
    """This is your working StreamJob class. It remains unchanged."""
    def __init__(self, src_path: str, dst_paths: List[str], job_id: Optional[str] = None):
        super().__init__(src_path, dst_paths, False, False, job_id)
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3, backoff_factor=0.5))
    def dispatch(self, dataplane: "Dataplane", **kwargs) -> Generator[Chunk, None, None]:
        logger.fs.info(f"Dispatching stream job {self.uuid}")
        trigger_chunk = Chunk(src_key=self.src_prefix, dest_key=self.dst_prefixes[0], chunk_id=uuid.uuid4().hex, chunk_length_bytes=0, partition_id="kafka_stream")
        trigger_request = ChunkRequest(chunk=trigger_chunk, src_region=dataplane.topology.src_region_tag, dst_region=dataplane.topology.dest_region_tags[0])
        def chunk_request(server, req):
            body = json.dumps([req.as_dict()]).encode()
            return self.http_pool.request("POST", f"{server.gateway_api_url}/api/v1/chunk_requests", body=body, headers={"Content-Type": "application/json"})
        for server in dataplane.source_gateways():
            retry_backoff(partial(chunk_request, server, trigger_request), max_retries=5)
        yield trigger_chunk
    def finalize(self): pass
    def verify(self): pass
    def size_gb(self): return 0


@ dataclass
class S3ToKafkaJob(TransferJob):
    """Generate 1MB chunk requests from S3 and dispatch to source gateways for Kafka writing downstream."""
    def __init__(self, src_path: str, dst_path: str, chunk_size_mb: int = 1, job_id: Optional[str] = None):
        super().__init__(src_path, [dst_path], False, False, job_id)
        self.chunk_size_bytes = int(chunk_size_mb * MB)
        self._http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), timeout=urllib3.util.Timeout(connect=10.0, read=None))

    @property
    def http_pool(self):
        return self._http_pool

    def _iter_src_objects(self) -> Generator[ObjectStoreObject, None, None]:
        # If exact object, yield it; else list prefix
        size = None
        try:
            size = self.src_iface.get_obj_size(self.src_prefix)
        except Exception:
            size = None
        if size is not None:
            yield ObjectStoreObject(
                key=self.src_prefix,
                provider=self.src_iface.provider if hasattr(self.src_iface, "provider") else None,
                bucket=self.src_iface.bucket(),
                size=size,
            )
            return
        # Otherwise iterate prefix
        for obj in self.src_iface.list_objects(self.src_prefix):
            yield obj

    def dispatch(self, dataplane: "Dataplane", dispatch_batch_size: int = 256, transfer_config: Optional[TransferConfig] = None) -> Generator[Chunk, None, None]:
        job_partition = "s3_to_kafka"

        def chunk_request(server, requests_batch):
            body = json.dumps([req.as_dict() for req in requests_batch]).encode()
            return self.http_pool.request("POST", f"{server.gateway_api_url}/api/v1/chunk_requests", body=body, headers={"Content-Type": "application/json"})

        src_gateways = dataplane.source_gateways()
        assert len(src_gateways) >= 1, "Expected at least one source gateway"

        batch_reqs: List[ChunkRequest] = []
        for obj in self._iter_src_objects():
            obj_size = obj.size if obj.size is not None else self.src_iface.get_obj_size(obj.key)
            if obj_size is None or obj_size == 0:
                continue
            part_number = 1
            offset = 0
            while offset < obj_size:
                chunk_len = min(self.chunk_size_bytes, obj_size - offset)
                chunk = Chunk(
                    src_key=obj.key,
                    dest_key=obj.key,
                    chunk_id=uuid.uuid4().hex,
                    chunk_length_bytes=chunk_len,
                    partition_id=job_partition,
                    md5_hash=None,
                    multi_part=False,
                    file_offset_bytes=offset,
                    part_number=part_number,
                )
                req = ChunkRequest(
                    chunk=chunk,
                    src_region=dataplane.topology.src_region_tag,
                    dst_region=dataplane.topology.dest_region_tags[0],
                    src_object_store_bucket=self.src_iface.bucket(),
                )
                batch_reqs.append(req)
                yield chunk
                if len(batch_reqs) >= dispatch_batch_size:
                    for server in src_gateways:
                        retry_backoff(lambda: chunk_request(server, batch_reqs), max_retries=5)
                    batch_reqs.clear()
                offset += chunk_len
                part_number += 1
        if batch_reqs:
            for server in src_gateways:
                retry_backoff(lambda: chunk_request(server, batch_reqs), max_retries=5)
            batch_reqs.clear()

    def finalize(self):
        pass

    def verify(self):
        pass

    def size_gb(self):
        return 0