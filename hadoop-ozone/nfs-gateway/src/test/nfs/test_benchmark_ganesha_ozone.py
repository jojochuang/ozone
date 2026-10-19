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

"""Unit tests for mounted benchmark counters and server RSS sampling."""

import contextlib
import errno
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import benchmark_ganesha_ozone


class ProcessRssSamplerTest(unittest.TestCase):
    def test_mount_options_select_nfs_entry_after_synthetic_mount(self):
        mount_table = """\
container:/host /mnt/ozone fuse.fakeowner rw,relatime 0 0
server:/export /mnt/ozone nfs4 rw,vers=4.1,sec=krb5p,acregmin=0,acregmax=0,acdirmin=0,acdirmax=0 0 0
"""

        result = benchmark_ganesha_ozone.mount_options_for_path("/mnt/ozone", mount_table)

        self.assertEqual("server:/export", result["source"])
        self.assertIn("vers=4.1", result["options"])
        self.assertIn("sec=krb5p", result["options"])

    def test_reads_resident_memory_from_proc_status(self):
        status = "Name:\tjava\nVmSize:\t1000 kB\nVmRSS:\t1536 kB\n"
        with patch("builtins.open", mock_open(read_data=status)):
            self.assertEqual(1.5, benchmark_ganesha_ozone.process_rss_mib(42))

    def test_samples_peak_rss_for_each_named_process(self):
        with patch("benchmark_ganesha_ozone.process_rss_mib", return_value=64.0):
            sampler = benchmark_ganesha_ozone.ProcessRssSampler(
                [("ganesha", 42), ("om", 43)], interval_seconds=0.001)
            sampler.start()
            sampler.thread.join(timeout=0.02)
            sampler.stop()

        results = sampler.results()
        self.assertEqual(64.0, results["ganesha"]["peak_rss_mib"])
        self.assertEqual(64.0, results["om"]["peak_rss_mib"])
        self.assertGreaterEqual(results["ganesha"]["samples"], 2)
        self.assertGreaterEqual(results["om"]["samples"], 2)

    def test_empty_process_list_returns_empty_results(self):
        self.assertEqual({}, benchmark_ganesha_ozone.ProcessRssSampler([]).results())


class BenchmarkMetricsTest(unittest.TestCase):
    def test_cleanup_failure_does_not_hide_transfer_failure(self):
        with tempfile.TemporaryDirectory() as mount:
            def fail_transfer(directory, total_bytes, chunk_bytes):
                (directory / "sequential.bin").write_bytes(b"partial")
                raise IOError("sequential readback mismatch")

            stderr = io.StringIO()
            with patch("benchmark_ganesha_ozone.read_mountstats", return_value={}):
                with patch("benchmark_ganesha_ozone.ProcessRssSampler"):
                    with patch("benchmark_ganesha_ozone.sequential_transfer", side_effect=fail_transfer):
                        with patch.object(Path, "unlink", side_effect=OSError(errno.EBUSY, "busy")):
                            with contextlib.redirect_stderr(stderr):
                                with self.assertRaisesRegex(IOError, "sequential readback mismatch"):
                                    benchmark_ganesha_ozone.run_benchmark(
                                        Path(mount), 1024, 512, 1, 1, [])

            self.assertIn("benchmark cleanup failed", stderr.getvalue())

    def test_reads_per_operation_counts_for_requested_nfs_mount(self):
        mountstats = """device ganesha:/ozone mounted on /mnt/ozone\\040with\\040space with fstype nfs4 statvers=1.1
opts: rw,vers=4.1
per-op statistics
READ: 17 17 1024 0 0 1 2 3
WRITE: 9 9 512 0 0 1 2 3
device ganesha:/other mounted on /mnt/other with fstype nfs4 statvers=1.1
opts: rw,vers=4.1
per-op statistics
READ: 100 100 2048 0 0 1 2 3
"""
        with patch("builtins.open", mock_open(read_data=mountstats)):
            operations = benchmark_ganesha_ozone.read_mountstats(
                "/mnt/ozone with space", "/mountstats")

        self.assertEqual({"READ": 17, "WRITE": 9}, operations)

    def test_reads_om_request_counters_from_jmx_bean(self):
        payload = {
            "beans": [
                {"name": "Hadoop:service=OzoneManager,name=OMMetrics",
                 "NumKeyOps": 42, "NumFSOps": 17, "tag.Hostname": "om-1", "OtherMetric": 1.5},
                {"name": "Hadoop:service=OzoneManager,name=OMInfo", "NumKeyOps": 100},
            ]
        }

        self.assertEqual({"NumKeyOps": 42, "NumFSOps": 17},
                         benchmark_ganesha_ozone.parse_om_jmx_metrics(payload))

    def test_reads_om_request_counters_from_prometheus(self):
        payload = """\
# TYPE om_metrics_num_key_ops counter
om_metrics_num_key_ops{context="om",servername="om1"} 42
om_metrics_num_key_ops{context="om",servername="om2"} 3
om_metrics_num_fs_ops{context="om"} 17
other_metrics_num_key_ops 99
"""

        self.assertEqual({"NumKeyOps": 45, "NumFSOps": 17},
                         benchmark_ganesha_ozone.parse_om_prometheus_metrics(payload))

    def test_prometheus_request_uses_bearer_token(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"om_metrics_num_key_ops 42\n"
        with patch("benchmark_ganesha_ozone.urlopen", return_value=response) as urlopen:
            metrics = benchmark_ganesha_ozone.read_om_prometheus_metrics(
                "http://om:9874/prom", "secret-token")

        request = urlopen.call_args.args[0]
        self.assertEqual("Bearer secret-token", request.get_header("Authorization"))
        self.assertEqual({"NumKeyOps": 42}, metrics)

    def test_prometheus_parser_requires_om_metrics(self):
        with self.assertRaisesRegex(ValueError, "OMMetrics counters not found"):
            benchmark_ganesha_ozone.parse_om_prometheus_metrics("other_metric 1\n")

    def test_counter_delta_excludes_resets_and_reports_them(self):
        before = {"NumKeyOps": 10, "NumFSOps": 5, "NumKeyLists": 3}
        after = {"NumKeyOps": 16, "NumFSOps": 9, "NumKeyLists": 1}

        self.assertEqual({"NumFSOps": 4, "NumKeyOps": 6},
                         benchmark_ganesha_ozone.counter_delta(before, after))
        self.assertEqual(["NumKeyLists"], benchmark_ganesha_ozone.counter_resets(before, after))


if __name__ == "__main__":
    unittest.main()
