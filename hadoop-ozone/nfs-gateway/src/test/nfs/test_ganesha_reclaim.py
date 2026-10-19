# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify NFSv4 open state across a Ganesha process restart or instance replacement."""

import argparse
import os
from pathlib import Path
import shlex
import subprocess
import unittest
import uuid

from test_ganesha_write import require_nfs41_krb5p_mount


def read_host_identity(command, timeout_seconds):
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds)
    identity = result.stdout.strip()
    if not identity:
        raise ValueError("host identity command returned no identity")
    return identity


class GaneshaReclaimContract(unittest.TestCase):
    mount = None
    restart_command = None
    restart_timeout_seconds = 90
    serving_host_id_command = None
    recovery_root_identity_command = None

    def test_open_state_survives_server_restart(self):
        serving_host_id = None
        recovery_root_identity = None
        if self.serving_host_id_command is not None:
            serving_host_id = read_host_identity(
                self.serving_host_id_command, self.restart_timeout_seconds)
        if self.recovery_root_identity_command is not None:
            recovery_root_identity = read_host_identity(
                self.recovery_root_identity_command, self.restart_timeout_seconds)

        path = self.mount / ("reclaim-contract-" + uuid.uuid4().hex)
        initial = b"stable data before Ganesha restart"
        appended = b"; write after reclaim"

        def cleanup():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        self.addCleanup(cleanup)
        with path.open("x+b", buffering=0) as stream:
            self.assertEqual(len(initial), stream.write(initial))
            stream.flush()
            os.fsync(stream.fileno())

            subprocess.run(
                self.restart_command,
                check=True,
                timeout=self.restart_timeout_seconds)

            if serving_host_id is not None:
                replacement_host_id = read_host_identity(
                    self.serving_host_id_command, self.restart_timeout_seconds)
                self.assertNotEqual(
                    serving_host_id,
                    replacement_host_id,
                    "NFS endpoint is still served by the original host")
            if recovery_root_identity is not None:
                replacement_recovery_root_identity = read_host_identity(
                    self.recovery_root_identity_command, self.restart_timeout_seconds)
                self.assertEqual(
                    recovery_root_identity,
                    replacement_recovery_root_identity,
                    "RecoveryRoot identity changed after failover")

            stream.seek(0)
            self.assertEqual(initial, stream.read())
            stream.seek(0, 2)
            self.assertEqual(len(appended), stream.write(appended))
            stream.flush()
            os.fsync(stream.fileno())

        self.assertEqual(initial + appended, path.read_bytes())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument(
        "--restart-command",
        required=True,
        help="Ganesha restart command, split into arguments without a shell")
    parser.add_argument("--restart-timeout-seconds", type=int, default=90)
    parser.add_argument(
        "--serving-host-id-command",
        help="Optional command that prints the stable identity of the host serving this NFS endpoint")
    parser.add_argument(
        "--recovery-root-identity-command",
        help="Command that prints the stable identity of the Ganesha RecoveryRoot storage")
    arguments = parser.parse_args()
    GaneshaReclaimContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    GaneshaReclaimContract.restart_command = shlex.split(arguments.restart_command)
    GaneshaReclaimContract.restart_timeout_seconds = arguments.restart_timeout_seconds
    if arguments.serving_host_id_command is not None:
        GaneshaReclaimContract.serving_host_id_command = shlex.split(
            arguments.serving_host_id_command)
        if not GaneshaReclaimContract.serving_host_id_command:
            parser.error("--serving-host-id-command must not be empty")
    if arguments.recovery_root_identity_command is not None:
        if arguments.serving_host_id_command is None:
            parser.error("--recovery-root-identity-command requires --serving-host-id-command")
        GaneshaReclaimContract.recovery_root_identity_command = shlex.split(
            arguments.recovery_root_identity_command)
        if not GaneshaReclaimContract.recovery_root_identity_command:
            parser.error("--recovery-root-identity-command must not be empty")
    if not GaneshaReclaimContract.restart_command:
        parser.error("--restart-command must not be empty")
    unittest.main(argv=[__file__], verbosity=2)
