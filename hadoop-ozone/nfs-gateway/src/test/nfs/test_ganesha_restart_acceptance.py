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

"""Repeat the mounted byte-exact publication check after gateway restarts."""

import argparse
import shlex
import subprocess
import sys
import time
import unittest
from pathlib import Path

import test_ganesha_ozone


def wait_for_command(command, timeout_seconds, poll_interval_seconds, command_timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            result = subprocess.run(
                command,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=command_timeout_seconds)
        except subprocess.TimeoutExpired:
            result = None
        if result is not None and result.returncode == 0:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(poll_interval_seconds, remaining))


def read_command_output(command, timeout_seconds):
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds)
    value = result.stdout.strip()
    if not value:
        raise ValueError("gateway instance command returned no identity")
    return value


def run_publication_check(mount, ozone_uri, ozone_cli):
    test_ganesha_ozone.GaneshaOzoneContract.mount = (
        test_ganesha_ozone.require_nfs41_krb5p_mount(mount))
    test_ganesha_ozone.GaneshaOzoneContract.ozone_uri = ozone_uri
    test_ganesha_ozone.GaneshaOzoneContract.ozone_cli = ozone_cli
    suite = unittest.TestSuite([
        test_ganesha_ozone.GaneshaOzoneContract(
            "test_32_megabyte_upload_is_published_byte_exactly")])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


def run_restart_cycles(arguments):
    mount = test_ganesha_ozone.require_nfs41_krb5p_mount(arguments.mount)
    for cycle in range(1, arguments.cycles + 1):
        print("Restart acceptance cycle {}/{}".format(cycle, arguments.cycles), flush=True)
        previous_instance = read_command_output(
            arguments.instance_id_command,
            arguments.instance_id_command_timeout_seconds)
        subprocess.run(
            arguments.restart_command,
            check=True,
            timeout=arguments.restart_timeout_seconds)
        if not wait_for_command(
                arguments.ready_command,
                arguments.ready_timeout_seconds,
                arguments.ready_poll_interval_seconds,
                arguments.ready_command_timeout_seconds):
            print("Gateway readiness command timed out", file=sys.stderr)
            return False
        current_instance = read_command_output(
            arguments.instance_id_command,
            arguments.instance_id_command_timeout_seconds)
        if current_instance == previous_instance:
            print("Gateway instance identity did not change after restart", file=sys.stderr)
            return False
        if not run_publication_check(mount, arguments.ozone_uri, arguments.ozone_cli):
            return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--ozone-uri", required=True,
                        help="OzoneFS bucket root, for example ofs://om-host/volume/bucket")
    parser.add_argument("--ozone-cli", default="ozone", help="Path to the Ozone CLI executable")
    parser.add_argument("--restart-command", required=True,
                        help="Gateway restart command, split into arguments without a shell")
    parser.add_argument("--instance-id-command", required=True,
                        help="Command that prints a unique identity for the serving gateway process")
    parser.add_argument("--ready-command", required=True,
                        help="Command that exits successfully only when the gateway can accept requests")
    parser.add_argument("--restart-timeout-seconds", type=int, default=120)
    parser.add_argument("--instance-id-command-timeout-seconds", type=int, default=10)
    parser.add_argument("--ready-timeout-seconds", type=int, default=120)
    parser.add_argument("--ready-poll-interval-seconds", type=float, default=1)
    parser.add_argument("--ready-command-timeout-seconds", type=int, default=10)
    parser.add_argument("--cycles", type=int, default=1,
                        help="Number of restart and publication cycles to run")
    arguments = parser.parse_args()
    arguments.restart_command = shlex.split(arguments.restart_command)
    arguments.instance_id_command = shlex.split(arguments.instance_id_command)
    arguments.ready_command = shlex.split(arguments.ready_command)
    if not arguments.restart_command:
        parser.error("--restart-command must not be empty")
    if not arguments.instance_id_command:
        parser.error("--instance-id-command must not be empty")
    if not arguments.ready_command:
        parser.error("--ready-command must not be empty")
    if (arguments.restart_timeout_seconds <= 0 or arguments.ready_timeout_seconds <= 0
            or arguments.instance_id_command_timeout_seconds <= 0):
        parser.error("restart, readiness, and instance identity timeouts must be positive")
    if (arguments.ready_poll_interval_seconds <= 0
            or arguments.ready_command_timeout_seconds <= 0
            or arguments.cycles <= 0):
        parser.error("readiness intervals, command timeout, and cycle count must be positive")
    sys.exit(0 if run_restart_cycles(arguments) else 1)
