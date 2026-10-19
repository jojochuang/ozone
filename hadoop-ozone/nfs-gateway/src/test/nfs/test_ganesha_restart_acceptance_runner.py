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

"""Unit tests for the restart-window mounted acceptance runner."""

import subprocess
import unittest
from unittest.mock import patch

import test_ganesha_restart_acceptance


class GaneshaRestartAcceptanceRunnerTest(unittest.TestCase):
    def test_read_command_output_trims_gateway_identity(self):
        with patch.object(test_ganesha_restart_acceptance.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "gateway-123\n", "")

            identity = test_ganesha_restart_acceptance.read_command_output(["gateway-pid"], 10)

        self.assertEqual("gateway-123", identity)
        self.assertTrue(run.call_args.kwargs["check"])
        self.assertEqual(10, run.call_args.kwargs["timeout"])

    def test_read_command_output_rejects_empty_gateway_identity(self):
        with patch.object(test_ganesha_restart_acceptance.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "  \n", "")

            with self.assertRaisesRegex(ValueError, "no identity"):
                test_ganesha_restart_acceptance.read_command_output(["gateway-pid"], 10)

    def test_wait_for_command_retries_until_ready(self):
        with patch.object(test_ganesha_restart_acceptance.subprocess, "run") as run:
            run.side_effect = [
                subprocess.CompletedProcess([], 1),
                subprocess.CompletedProcess([], 0),
            ]
            with patch.object(test_ganesha_restart_acceptance.time, "sleep") as sleep:
                ready = test_ganesha_restart_acceptance.wait_for_command(
                    ["gateway-ready"], 10, 1, 2)

        self.assertTrue(ready)
        self.assertEqual(2, run.call_count)
        run.assert_called_with(
            ["gateway-ready"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=2)
        sleep.assert_called_once_with(1)

    def test_wait_for_command_returns_false_after_timeout(self):
        with patch.object(test_ganesha_restart_acceptance.time, "monotonic", side_effect=[0, 0]):
            with patch.object(test_ganesha_restart_acceptance.subprocess, "run") as run:
                run.return_value = subprocess.CompletedProcess([], 1)
                with patch.object(test_ganesha_restart_acceptance.time, "sleep") as sleep:
                    ready = test_ganesha_restart_acceptance.wait_for_command(
                        ["gateway-ready"], 0, 1, 2)

        self.assertFalse(ready)
        run.assert_called_once()
        sleep.assert_not_called()

    def test_restart_cycle_waits_for_readiness_before_publication(self):
        arguments = type("Arguments", (), {
            "mount": "/mnt/test",
            "cycles": 1,
            "restart_command": ["restart-nfs"],
            "restart_timeout_seconds": 60,
            "instance_id_command": ["gateway-pid"],
            "instance_id_command_timeout_seconds": 10,
            "ready_command": ["gateway-ready"],
            "ready_timeout_seconds": 60,
            "ready_poll_interval_seconds": 1,
            "ready_command_timeout_seconds": 10,
            "ozone_uri": "ofs://om-host/volume/bucket",
            "ozone_cli": "/opt/hadoop/bin/ozone",
        })()
        with patch.object(test_ganesha_restart_acceptance.test_ganesha_ozone,
                          "require_nfs41_krb5p_mount", return_value="/mnt/test"):
            with patch.object(test_ganesha_restart_acceptance.subprocess, "run") as run:
                with patch.object(test_ganesha_restart_acceptance,
                                  "wait_for_command", return_value=True) as wait:
                    with patch.object(test_ganesha_restart_acceptance,
                                      "read_command_output", side_effect=("pid-1", "pid-2")) as identity:
                        with patch.object(test_ganesha_restart_acceptance,
                                          "run_publication_check", return_value=True) as publish:
                            succeeded = test_ganesha_restart_acceptance.run_restart_cycles(arguments)

        self.assertTrue(succeeded)
        run.assert_called_once_with(
            ["restart-nfs"], check=True, timeout=60)
        wait.assert_called_once_with(["gateway-ready"], 60, 1, 10)
        self.assertEqual(2, identity.call_count)
        publish.assert_called_once_with(
            "/mnt/test", "ofs://om-host/volume/bucket", "/opt/hadoop/bin/ozone")

    def test_restart_cycle_stops_when_gateway_does_not_become_ready(self):
        arguments = type("Arguments", (), {
            "mount": "/mnt/test",
            "cycles": 1,
            "restart_command": ["restart-nfs"],
            "restart_timeout_seconds": 60,
            "instance_id_command": ["gateway-pid"],
            "instance_id_command_timeout_seconds": 10,
            "ready_command": ["gateway-ready"],
            "ready_timeout_seconds": 2,
            "ready_poll_interval_seconds": 1,
            "ready_command_timeout_seconds": 10,
            "ozone_uri": "ofs://om-host/volume/bucket",
            "ozone_cli": "ozone",
        })()
        with patch.object(test_ganesha_restart_acceptance.test_ganesha_ozone,
                          "require_nfs41_krb5p_mount", return_value="/mnt/test"):
            with patch.object(test_ganesha_restart_acceptance.subprocess, "run"):
                with patch.object(test_ganesha_restart_acceptance,
                                  "wait_for_command", return_value=False):
                    with patch.object(test_ganesha_restart_acceptance,
                                      "run_publication_check") as publish:
                        succeeded = test_ganesha_restart_acceptance.run_restart_cycles(arguments)

        self.assertFalse(succeeded)
        publish.assert_not_called()

    def test_restart_cycle_rejects_unchanged_gateway_instance(self):
        arguments = type("Arguments", (), {
            "mount": "/mnt/test",
            "cycles": 1,
            "restart_command": ["restart-nfs"],
            "restart_timeout_seconds": 60,
            "instance_id_command": ["gateway-pid"],
            "instance_id_command_timeout_seconds": 10,
            "ready_command": ["gateway-ready"],
            "ready_timeout_seconds": 60,
            "ready_poll_interval_seconds": 1,
            "ready_command_timeout_seconds": 10,
            "ozone_uri": "ofs://om-host/volume/bucket",
            "ozone_cli": "ozone",
        })()
        with patch.object(test_ganesha_restart_acceptance.test_ganesha_ozone,
                          "require_nfs41_krb5p_mount", return_value="/mnt/test"):
            with patch.object(test_ganesha_restart_acceptance.subprocess, "run"):
                with patch.object(test_ganesha_restart_acceptance,
                                  "read_command_output", return_value="same-instance"):
                    with patch.object(test_ganesha_restart_acceptance,
                                      "wait_for_command", return_value=True):
                        with patch.object(test_ganesha_restart_acceptance,
                                          "run_publication_check") as publish:
                            succeeded = test_ganesha_restart_acceptance.run_restart_cycles(arguments)

        self.assertFalse(succeeded)
        publish.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
