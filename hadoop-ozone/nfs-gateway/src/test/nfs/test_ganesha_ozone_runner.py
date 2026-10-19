# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the mounted Ozone publication runner."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import test_ganesha_ozone


class GaneshaOzoneRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.contract = test_ganesha_ozone.GaneshaOzoneContract()
        self.contract.mount = Path(self.temporary_directory.name)

    def test_configured_files_match_ozonefs_bytes(self):
        expected = {
            "ec-file": b"EC data",
            "encrypted-file": b"KMS encrypted data",
        }
        for name, data in expected.items():
            (self.contract.mount / name).write_bytes(data)
        self.contract.read_keys = tuple(expected)
        self.contract.read_ozone_file = Mock(side_effect=expected.__getitem__)

        self.contract.test_configured_ozone_files_read_through_ganesha()

        self.assertEqual(2, self.contract.read_ozone_file.call_count)

    def test_configured_file_key_cannot_escape_export(self):
        self.contract.read_keys = ("../outside",)
        self.contract.read_ozone_file = Mock()

        with self.assertRaisesRegex(AssertionError, "relative path"):
            self.contract.test_configured_ozone_files_read_through_ganesha()

        self.contract.read_ozone_file.assert_not_called()

    def test_cp_preserve_metadata_failure_keeps_uploaded_bytes(self):
        self.contract.read_ozone_file = Mock(side_effect=lambda name: (
            self.contract.mount / name).read_bytes())

        def copy_with_unsupported_times(command, **kwargs):
            Path(command[-1]).write_bytes(Path(command[-2]).read_bytes())
            return subprocess.CompletedProcess(command, 1, "", "Invalid argument")

        with patch.object(test_ganesha_ozone.subprocess, "run", side_effect=copy_with_unsupported_times) as run:
            self.contract.test_cp_preserve_metadata_reports_unsupported_times_after_upload()

        self.assertEqual("-p", run.call_args.args[0][1])
        self.assertTrue(run.call_args.kwargs["capture_output"])
        self.assertEqual(1, self.contract.read_ozone_file.call_count)

    def test_nfs_mount_options_ignore_docker_synthetic_mount(self):
        mount = Path("/mnt/ozone")
        mount_table = """\
container:/host /mnt/ozone fuse.fakeowner rw,relatime 0 0
server:/export /mnt/ozone nfs4 rw,vers=4.1,sec=krb5p,acregmin=0,acregmax=0,acdirmin=0,acdirmax=0 0 0
"""

        options = test_ganesha_ozone.mount_options_for_nfs_path(mount, mount_table)

        self.assertIn("vers=4.1", options)
        self.assertIn("sec=krb5p", options)
        self.assertIn("acregmin=0", options)

    def test_distinct_exports_keep_same_name_keys_isolated(self):
        isolated_directory = tempfile.TemporaryDirectory()
        self.addCleanup(isolated_directory.cleanup)
        self.contract.isolated_mount = Path(isolated_directory.name)
        self.contract.isolated_ozone_uri = "ofs://om-host/volume/isolated"
        self.contract.ozone_uri = "ofs://om-host/volume/primary"
        self.contract.ozone_file_exists_at = Mock(return_value=False)
        self.contract.read_ozone_file_at = Mock(side_effect=lambda uri: (
            b"primary Ozone export" if "/primary/" in uri else b"isolated Ozone export"))

        self.contract.test_exports_do_not_expose_each_others_keys()

        self.contract.ozone_file_exists_at.assert_called_once()
        isolated_uri = self.contract.ozone_file_exists_at.call_args.args[0]
        self.assertTrue(isolated_uri.startswith("ofs://om-host/volume/isolated/"))
        self.assertEqual(2, self.contract.read_ozone_file_at.call_count)

    def test_gateway_renewal_contract_waits_and_verifies_both_writes(self):
        self.contract.ozone_uri = "ofs://om-host/volume/bucket"
        self.contract.gateway_tgt_lifetime_seconds = 120
        self.contract.read_ozone_file = Mock(side_effect=lambda name: (
            self.contract.mount / name).read_bytes())

        with patch.object(test_ganesha_ozone.time, "sleep") as sleep:
            self.contract.test_gateway_write_remains_available_after_tgt_expiry()

        sleep.assert_called_once_with(130)
        self.assertEqual(2, self.contract.read_ozone_file.call_count)

    def test_gateway_renewal_contract_skips_without_short_lived_tgt(self):
        with self.assertRaises(unittest.SkipTest):
            self.contract.test_gateway_write_remains_available_after_tgt_expiry()


if __name__ == "__main__":
    unittest.main(verbosity=2)
