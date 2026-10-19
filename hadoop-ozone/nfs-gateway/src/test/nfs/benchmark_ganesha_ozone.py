# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Measure sequential transfer and small-file latency on a mounted Ozone export."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import resource
import statistics
import sys
import threading
import time
from urllib.request import Request, urlopen
import uuid

from test_ganesha_write import require_nfs41_krb5p_mount


def write_all(descriptor, data):
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            raise OSError("NFS write made no progress")
        offset += written


def digest_repeated(data, total_bytes):
    digest = hashlib.sha256()
    remaining = total_bytes
    while remaining:
        chunk = data[:min(remaining, len(data))]
        digest.update(chunk)
        remaining -= len(chunk)
    return digest.digest()


def percentiles(samples):
    ordered = sorted(samples)
    return {
        "p50_ms": round(statistics.median(ordered) * 1000, 3),
        "p95_ms": round(ordered[math.ceil(0.95 * len(ordered)) - 1] * 1000, 3),
    }


def mount_options_for_path(mount, mount_table):
    for line in mount_table.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        path = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), fields[1])
        if path == str(mount) and fields[2] in ("nfs", "nfs4"):
            return {"source": fields[0], "options": fields[3].split(",")}
    raise ValueError("NFS mount disappeared before the benchmark started")


def mount_options(mount):
    with open("/proc/mounts", encoding="utf-8") as mounts:
        return mount_options_for_path(mount, mounts.read())


def read_mountstats(mount, mountstats_path="/proc/self/mountstats"):
    with open(mountstats_path, encoding="utf-8") as mountstats:
        lines = mountstats.readlines()

    for index, line in enumerate(lines):
        if not line.startswith("device "):
            continue
        fields = line.split()
        if len(fields) < 8 or fields[7] not in ("nfs", "nfs4"):
            continue
        mounted_path = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), fields[4])
        if mounted_path != str(mount):
            continue
        for stats_index in range(index + 1, len(lines)):
            if lines[stats_index].startswith("device "):
                break
            if lines[stats_index].strip() != "per-op statistics":
                continue
            operations = {}
            for stats_line in lines[stats_index + 1:]:
                if stats_line.startswith("device ") or not stats_line.strip():
                    break
                match = re.match(r"\s*([^:]+):\s+(\d+)", stats_line)
                if match:
                    operations[match.group(1).strip()] = int(match.group(2))
            if operations:
                return operations
    raise ValueError("NFS per-operation statistics not found for mount {}".format(mount))


def read_om_jmx_metrics(jmx_url):
    with urlopen(jmx_url, timeout=5) as response:
        payload = json.load(response)
    return parse_om_jmx_metrics(payload)


def read_om_prometheus_metrics(prometheus_url, token):
    request = Request(prometheus_url, headers={"Authorization": "Bearer " + token})
    with urlopen(request, timeout=5) as response:
        payload = response.read().decode("utf-8")
    return parse_om_prometheus_metrics(payload)


def parse_om_jmx_metrics(payload):
    beans = payload.get("beans", [])
    bean = next((item for item in beans
                 if "name=OMMetrics" in item.get("name", "")), None)
    if bean is None:
        raise ValueError("OMMetrics bean not found in JMX response")
    return {
        name: value for name, value in bean.items()
        if name.startswith("Num") and isinstance(value, (int, float))
        and not isinstance(value, bool)
    }


def parse_om_prometheus_metrics(payload):
    metrics = {}
    for line in payload.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(
            r"om_metrics_(num_[a-z0-9_]+)(?:\{[^}]*\})?\s+"
            r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)(?:\s+\d+)?$",
            line)
        if not match:
            continue
        acronyms = {"fs": "FS"}
        words = match.group(1)[4:].split("_")
        name = "Num" + "".join(acronyms.get(word, word.title()) for word in words)
        value = float(match.group(2))
        if value.is_integer():
            value = int(value)
        metrics[name] = metrics.get(name, 0) + value
    if not metrics:
        raise ValueError("OMMetrics counters not found in Prometheus response")
    return metrics


def counter_delta(before, after):
    return {
        name: after[name] - before[name]
        for name in sorted(before.keys() & after.keys())
        if after[name] > before[name]
    }


def counter_resets(before, after):
    return sorted(name for name in before.keys() & after.keys() if after[name] < before[name])


def sequential_transfer(directory, total_bytes, chunk_bytes):
    path = directory / "sequential.bin"
    payload = b"Ozone-NFS-benchmark" * ((chunk_bytes + 18) // 19)
    payload = payload[:chunk_bytes]
    expected_digest = digest_repeated(payload, total_bytes)

    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        remaining = total_bytes
        chunks = math.ceil(total_bytes / len(payload))
        progress_every = max(1, math.ceil(chunks / 4))
        write_started = time.perf_counter()
        written_chunks = 0
        while remaining:
            chunk = payload[:min(remaining, len(payload))]
            write_all(descriptor, chunk)
            remaining -= len(chunk)
            written_chunks += 1
            if written_chunks % progress_every == 0 or remaining == 0:
                print("sequential write: {}/{} chunks".format(written_chunks, chunks),
                      file=sys.stderr, flush=True)
        write_seconds = time.perf_counter() - write_started

        sync_started = time.perf_counter()
        os.fsync(descriptor)
        sync_seconds = time.perf_counter() - sync_started
    finally:
        os.close(descriptor)

    descriptor = os.open(path, os.O_RDONLY)
    try:
        read_started = time.perf_counter()
        digest = hashlib.sha256()
        bytes_read = 0
        while True:
            chunk = os.read(descriptor, chunk_bytes)
            if not chunk:
                break
            bytes_read += len(chunk)
            digest.update(chunk)
        read_seconds = time.perf_counter() - read_started
    finally:
        os.close(descriptor)

    actual_digest = digest.digest()
    if bytes_read != total_bytes or actual_digest != expected_digest:
        raise IOError("sequential readback mismatch: expected {} bytes with SHA-256 {}, got {} bytes with SHA-256 {}"
                      .format(total_bytes, expected_digest.hex(), bytes_read, actual_digest.hex()))
    path.unlink()
    return {
        "bytes": total_bytes,
        "chunk_bytes": chunk_bytes,
        "write_seconds": round(write_seconds, 3),
        "write_mib_per_second_including_fsync": round(total_bytes / (write_seconds + sync_seconds) / (1024 ** 2), 3),
        "fsync_seconds": round(sync_seconds, 3),
        "read_seconds": round(read_seconds, 3),
        "read_mib_per_second": round(total_bytes / read_seconds / (1024 ** 2), 3),
        "readback_sha256": digest.hexdigest(),
    }


def small_files(directory, count, size_bytes):
    payload = b"n" * size_bytes
    samples = []
    for index in range(count):
        path = directory / ("small-{:05d}".format(index))
        started = time.perf_counter()
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            write_all(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if path.stat().st_size != size_bytes:
            raise IOError("small-file size mismatch for {}".format(path.name))
        path.unlink()
        samples.append(time.perf_counter() - started)
        if (index + 1) % max(1, count // 5) == 0 or index + 1 == count:
            print("small files: {}/{} complete".format(index + 1, count),
                  file=sys.stderr, flush=True)
    return {
        "files": count,
        "bytes_per_file": size_bytes,
        "create_write_fsync_delete_files_per_second": round(count / sum(samples), 3),
        "operation_latency": percentiles(samples),
    }


def peak_rss_mib():
    # Linux reports ru_maxrss in KiB.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)


def process_rss_mib(pid):
    with open("/proc/{}/status".format(pid), encoding="utf-8") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024, 3)
    raise ValueError("process {} has no VmRSS entry".format(pid))


class ProcessRssSampler:
    def __init__(self, processes, interval_seconds=0.25):
        self.processes = processes
        self.interval_seconds = interval_seconds
        self.samples = {name: [] for name, _ in processes}
        self.finished = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.stopped = False

    def start(self):
        for name, pid in self.processes:
            self.samples[name].append(process_rss_mib(pid))
        self.thread.start()

    def _sample(self):
        while not self.finished.wait(self.interval_seconds):
            for name, pid in self.processes:
                try:
                    self.samples[name].append(process_rss_mib(pid))
                except FileNotFoundError:
                    pass

    def stop(self):
        if self.stopped:
            return
        self.stopped = True
        self.finished.set()
        if self.thread.is_alive():
            self.thread.join()
        if self.thread.ident is not None:
            for name, pid in self.processes:
                try:
                    self.samples[name].append(process_rss_mib(pid))
                except FileNotFoundError:
                    pass

    def results(self):
        return {
            name: {
                "pid": pid,
                "peak_rss_mib": max(self.samples[name]),
                "samples": len(self.samples[name]),
            }
            for name, pid in self.processes
        }


def run_benchmark(mount, total_bytes, chunk_bytes, small_file_count, small_file_bytes, processes,
                  om_jmx_url=None, om_prometheus_url=None, om_prometheus_token=None):
    nfs_operations_before = read_mountstats(mount)
    if om_jmx_url:
        def read_om_metrics():
            return read_om_jmx_metrics(om_jmx_url)
    elif om_prometheus_url:
        def read_om_metrics():
            return read_om_prometheus_metrics(om_prometheus_url, om_prometheus_token)
    else:
        read_om_metrics = None
    om_metrics_before = read_om_metrics() if read_om_metrics else None
    work_dir = mount / ("nfs-benchmark-" + uuid.uuid4().hex)
    work_dir.mkdir()
    sampler = ProcessRssSampler(processes)
    try:
        sampler.start()
        print("starting sequential transfer: {} bytes".format(total_bytes),
              file=sys.stderr, flush=True)
        sequential = sequential_transfer(work_dir, total_bytes, chunk_bytes)
        print("starting small-file workload: {} files".format(small_file_count),
              file=sys.stderr, flush=True)
        small = small_files(work_dir, small_file_count, small_file_bytes)
        sampler.stop()
    finally:
        benchmark_failed = sys.exc_info()[0] is not None
        sampler.stop()
        cleanup_errors = []
        try:
            paths = list(work_dir.iterdir())
        except FileNotFoundError:
            paths = []
        except OSError as error:
            paths = []
            cleanup_errors.append("listing {}: {}".format(work_dir, error))
        for path in paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError as error:
                cleanup_errors.append("removing {}: {}".format(path, error))
        try:
            work_dir.rmdir()
        except FileNotFoundError:
            pass
        except OSError as error:
            cleanup_errors.append("removing {}: {}".format(work_dir, error))
        if cleanup_errors:
            message = "benchmark cleanup failed: " + "; ".join(cleanup_errors)
            if benchmark_failed:
                print(message, file=sys.stderr, flush=True)
            else:
                raise OSError(message)

    nfs_operations_after = read_mountstats(mount)
    result = {
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "mount": mount_options(mount),
        "sequential": sequential,
        "small_files": small,
        "client_process_peak_rss_mib": peak_rss_mib(),
        "server_processes": sampler.results(),
        "nfs_client_rpc_operations": counter_delta(nfs_operations_before, nfs_operations_after),
        "nfs_client_rpc_counter_resets": counter_resets(nfs_operations_before, nfs_operations_after),
    }
    if om_jmx_url:
        om_metrics_after = read_om_metrics()
        result["om_metrics_endpoint"] = "jmx"
        result["om_metrics"] = {
            "counter_delta": counter_delta(om_metrics_before, om_metrics_after),
            "counter_resets": counter_resets(om_metrics_before, om_metrics_after),
        }
    elif om_prometheus_url:
        om_metrics_after = read_om_metrics()
        result["om_metrics_endpoint"] = "prometheus"
        result["om_metrics"] = {
            "counter_delta": counter_delta(om_metrics_before, om_metrics_after),
            "counter_resets": counter_resets(om_metrics_before, om_metrics_after),
        }
    if om_jmx_url or om_prometheus_url:
        nfs_rpc_count = sum(result["nfs_client_rpc_operations"].values())
        key_ops = result["om_metrics"]["counter_delta"].get("NumKeyOps")
        if (key_ops is not None and nfs_rpc_count and not result["om_metrics"]["counter_resets"]
                and not result["nfs_client_rpc_counter_resets"]):
            result["om_metrics"]["key_ops_per_nfs_rpc"] = round(key_ops / nfs_rpc_count, 6)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--sequential-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--chunk-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--small-files", type=int, default=50)
    parser.add_argument("--small-file-bytes", type=int, default=4096)
    metrics_endpoint = parser.add_mutually_exclusive_group()
    metrics_endpoint.add_argument(
        "--om-jmx-url",
        help="optional OM /jmx URL for request-counter deltas (for example http://om:9874/jmx)",
    )
    metrics_endpoint.add_argument(
        "--om-prometheus-url",
        help="optional secure OM /prom URL (for example http://om:9874/prom)",
    )
    parser.add_argument(
        "--om-prometheus-token-file",
        help="file containing the bearer token configured with hdds.prometheus.endpoint.token",
    )
    parser.add_argument(
        "--process", action="append", default=[], metavar="NAME=PID",
        help="sample peak RSS for a local Linux process; repeat for Ganesha, gateway, and OM",
    )
    arguments = parser.parse_args()
    if min(arguments.sequential_bytes, arguments.chunk_bytes,
           arguments.small_files, arguments.small_file_bytes) <= 0:
        parser.error("benchmark sizes and counts must be positive")
    if arguments.om_prometheus_url and not arguments.om_prometheus_token_file:
        parser.error("--om-prometheus-url requires --om-prometheus-token-file")
    if arguments.om_prometheus_token_file and not arguments.om_prometheus_url:
        parser.error("--om-prometheus-token-file requires --om-prometheus-url")
    prometheus_token = None
    if arguments.om_prometheus_token_file:
        prometheus_token = Path(arguments.om_prometheus_token_file).read_text(encoding="utf-8").strip()
        if not prometheus_token:
            parser.error("--om-prometheus-token-file must not be empty")
    mounted = require_nfs41_krb5p_mount(arguments.mount)
    processes = []
    process_names = set()
    for item in arguments.process:
        try:
            name, pid_text = item.split("=", 1)
            pid = int(pid_text)
        except ValueError:
            parser.error("--process must be NAME=PID")
        if not name or name in process_names or pid <= 0:
            parser.error("--process names must be non-empty and unique, with positive PIDs")
        process_names.add(name)
        processes.append((name, pid))
    print(json.dumps(run_benchmark(
        mounted,
        arguments.sequential_bytes,
        arguments.chunk_bytes,
        arguments.small_files,
        arguments.small_file_bytes,
        processes,
        arguments.om_jmx_url,
        arguments.om_prometheus_url,
        prometheus_token,
    ), indent=2, sort_keys=True))
