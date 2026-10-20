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

"""Unit tests for the mounted Ganesha reclaim contract."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import test_ganesha_reclaim


class GaneshaReclaimRunnerTest(unittest.TestCase):
    def test_serving_host_probe_requires_recovery_root_identity_probe(self):
        script = Path(test_ganesha_reclaim.__file__)
        result = subprocess.run(
            [sys.executable, str(script),
             "--mount", "/mnt/test",
             "--restart-command", "promote-standby",
             "--serving-host-id-command", "serving-host-id"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10)

        self.assertEqual(2, result.returncode)
        self.assertIn(
            "--serving-host-id-command requires --recovery-root-identity-command",
            result.stderr)

    def test_shared_recovery_root_requires_server_identity_probe(self):
        script = Path(test_ganesha_reclaim.__file__)
        result = subprocess.run(
            [sys.executable, str(script),
             "--mount", "/mnt/test",
             "--restart-command", "promote-standby",
             "--serving-host-id-command", "serving-host-id",
             "--recovery-root-identity-command", "recovery-root-id"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10)

        self.assertEqual(2, result.returncode)
        self.assertIn(
            "--recovery-root-identity-command requires --server-identity-command",
            result.stderr)

    def test_read_host_identity_trims_command_output(self):
        with patch.object(test_ganesha_reclaim.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, " host-a\n", "")

            identity = test_ganesha_reclaim.read_host_identity(["print-machine-id"], 10)

        self.assertEqual("host-a", identity)
        self.assertTrue(run.call_args.kwargs["check"])
        self.assertEqual(10, run.call_args.kwargs["timeout"])

    def test_read_host_identity_rejects_empty_output(self):
        with patch.object(test_ganesha_reclaim.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "  \n", "")

            with self.assertRaisesRegex(ValueError, "no identity"):
                test_ganesha_reclaim.read_host_identity(["print-machine-id"], 10)

    def test_contract_rejects_failover_that_keeps_serving_host(self):
        with tempfile.TemporaryDirectory() as mount:
            contract = test_ganesha_reclaim.GaneshaReclaimContract()
            contract.mount = Path(mount)
            contract.restart_command = ["promote-standby"]
            contract.serving_host_id_command = ["serving-host-id"]

            with patch.object(test_ganesha_reclaim, "read_host_identity", side_effect=("same", "same")):
                with patch.object(test_ganesha_reclaim.subprocess, "run") as run:
                    with self.assertRaisesRegex(AssertionError, "still served by the original host"):
                        contract.test_open_state_survives_server_restart()

            run.assert_called_once_with(
                ["promote-standby"], check=True, timeout=90)

    def test_contract_preserves_open_file_when_serving_host_changes(self):
        with tempfile.TemporaryDirectory() as mount:
            contract = test_ganesha_reclaim.GaneshaReclaimContract()
            contract.mount = Path(mount)
            contract.restart_command = ["promote-standby"]
            contract.serving_host_id_command = ["serving-host-id"]
            contract.recovery_root_identity_command = ["recovery-root-id"]
            contract.server_identity_command = ["server-identity"]

            with patch.object(test_ganesha_reclaim, "read_host_identity",
                              side_effect=("host-a", "shared-root", "server-v1", "host-b",
                                           "shared-root", "server-v1")):
                with patch.object(test_ganesha_reclaim.subprocess, "run") as run:
                    contract.test_open_state_survives_server_restart()

            run.assert_called_once_with(
                ["promote-standby"], check=True, timeout=90)

    def test_contract_rejects_changed_nfs_server_identity(self):
        with tempfile.TemporaryDirectory() as mount:
            contract = test_ganesha_reclaim.GaneshaReclaimContract()
            contract.mount = Path(mount)
            contract.restart_command = ["promote-standby"]
            contract.serving_host_id_command = ["serving-host-id"]
            contract.recovery_root_identity_command = ["recovery-root-id"]
            contract.server_identity_command = ["server-identity"]

            with patch.object(test_ganesha_reclaim, "read_host_identity",
                              side_effect=("host-a", "shared-root", "server-v1", "host-b",
                                           "shared-root", "server-v2")):
                with patch.object(test_ganesha_reclaim.subprocess, "run") as run:
                    with self.assertRaisesRegex(
                            AssertionError, "NFSv4 server identity changed after failover"):
                        contract.test_open_state_survives_server_restart()

            run.assert_called_once_with(
                ["promote-standby"], check=True, timeout=90)

    def test_contract_rejects_failover_with_different_recovery_root(self):
        with tempfile.TemporaryDirectory() as mount:
            contract = test_ganesha_reclaim.GaneshaReclaimContract()
            contract.mount = Path(mount)
            contract.restart_command = ["promote-standby"]
            contract.serving_host_id_command = ["serving-host-id"]
            contract.recovery_root_identity_command = ["recovery-root-id"]

            with patch.object(test_ganesha_reclaim, "read_host_identity",
                              side_effect=("host-a", "root-a", "host-b", "root-b")):
                with patch.object(test_ganesha_reclaim.subprocess, "run") as run:
                    with self.assertRaisesRegex(
                            AssertionError, "RecoveryRoot identity changed after failover"):
                        contract.test_open_state_survives_server_restart()

            run.assert_called_once_with(
                ["promote-standby"], check=True, timeout=90)


if __name__ == "__main__":
    unittest.main(verbosity=2)
