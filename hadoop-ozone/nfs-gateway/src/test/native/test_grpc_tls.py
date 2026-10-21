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

"""Live C++/Java mTLS and namespace/read contract against a mock-data Java service."""

import argparse
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest


class _TcpProxy:
    """Expose the loopback-only Java fixture to a bridge-networked Docker container."""

    def __init__(self, listen_address, target_address):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(listen_address)
        self.listener.listen()
        self.target_address = target_address
        self.thread = threading.Thread(target=self._accept, daemon=True)

    @property
    def port(self):
        return self.listener.getsockname()[1]

    def start(self):
        self.thread.start()

    def close(self):
        self.listener.close()
        self.thread.join(timeout=2)

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._relay, args=(client,), daemon=True).start()

    def _relay(self, client):
        try:
            backend = socket.create_connection(self.target_address, timeout=5)
        except OSError:
            client.close()
            return

        def forward(source, destination):
            try:
                while True:
                    data = source.recv(65536)
                    if not data:
                        break
                    destination.sendall(data)
            except OSError:
                pass
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass

        reverse = threading.Thread(target=forward, args=(client, backend), daemon=True)
        reverse.start()
        forward(backend, client)
        reverse.join(timeout=2)
        client.close()
        backend.close()


class GrpcTlsContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="ozone-nfs-tls-")
        cls.root = Path(cls.directory.name)
        cls.server = None
        try:
            for name in ("ca", "other-ca"):
                cls.command("openssl", "req", "-new", "-x509", "-newkey", "rsa:2048", "-nodes",
                            "-days", "1", "-subj", "/CN=" + name,
                            "-addext", "basicConstraints=critical,CA:TRUE",
                            "-keyout", str(cls.root / (name + ".key")), "-out", str(cls.root / (name + ".crt")))
            cls.issue("server", "serverAuth", "DNS:localhost,DNS:host.docker.internal")
            cls.issue("allowed", "clientAuth", "URI:ozone-nfs-ganesha")
            cls.issue("unlisted", "clientAuth", "URI:ozone-nfs-ganesha")
            cls.issue("expired", "clientAuth", "URI:ozone-nfs-ganesha", expired=True)
            cls.log = open(cls.root / "server.log", "w")
            server_command = [
                "java", "-cp", OPTIONS.java_classpath,
                "org.apache.hadoop.ozone.nfs.gateway.NfsTlsContractServer",
                str(cls.root / "server.crt"), str(cls.root / "server.key"), str(cls.root / "ca.crt"),
                str(cls.root / "allowed.crt"), str(cls.root / "port"), str(cls.root / "stop")]
            cls.capabilities_marker = cls.root / "ganesha-capabilities"
            server_command.extend([str(cls.capabilities_marker), "2" if OPTIONS.ganesha_image else "1"])
            cls.server = subprocess.Popen(server_command, stdout=cls.log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 30
            while not (cls.root / "port").exists():
                if cls.server.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("TLS server failed: " + (cls.root / "server.log").read_text()[-4000:])
                time.sleep(0.05)
            cls.port = int((cls.root / "port").read_text())
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def command(cls, *arguments):
        result = subprocess.run(arguments, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode(errors="replace"))

    @classmethod
    def issue(cls, name, usage, alternative_name, days="1", expired=False):
        cls.command("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=" + name,
                    "-keyout", str(cls.root / (name + ".key")), "-out", str(cls.root / (name + ".csr")))
        extension = cls.root / (name + ".ext")
        extension.write_text("[ v3_req ]\nbasicConstraints=critical,CA:FALSE\nextendedKeyUsage=" + usage
                             + "\nsubjectAltName=" + alternative_name + "\n")
        if expired:
            ca_config = cls.root / "ca.cnf"
            ca_config.write_text("[ ca ]\n"
                                 "default_ca = local_ca\n"
                                 "[ local_ca ]\n"
                                 "database = " + str(cls.root / "ca.index") + "\n"
                                 "new_certs_dir = " + str(cls.root / "newcerts") + "\n"
                                 "certificate = " + str(cls.root / "ca.crt") + "\n"
                                 "private_key = " + str(cls.root / "ca.key") + "\n"
                                 "serial = " + str(cls.root / "ca.srl") + "\n"
                                 "default_md = sha256\n"
                                 "default_days = 1\n"
                                 "policy = policy_any\n"
                                 "copy_extensions = copy\n"
                                 "[ policy_any ]\n"
                                 "commonName = supplied\n")
            (cls.root / "ca.index").write_text("")
            (cls.root / "newcerts").mkdir()
            cls.command("openssl", "ca", "-batch", "-config", str(ca_config),
                        "-in", str(cls.root / (name + ".csr")), "-out", str(cls.root / (name + ".crt")),
                        "-startdate", "20200101000000Z", "-enddate", "20200102000000Z",
                        "-extfile", str(extension), "-extensions", "v3_req")
        else:
            cls.command("openssl", "x509", "-req", "-in", str(cls.root / (name + ".csr")),
                        "-CA", str(cls.root / "ca.crt"), "-CAkey", str(cls.root / "ca.key"), "-CAcreateserial",
                        "-days", days, "-extfile", str(extension), "-extensions", "v3_req",
                        "-out", str(cls.root / (name + ".crt")))

    @classmethod
    def tearDownClass(cls):
        if cls.server is not None:
            (cls.root / "stop").touch()
            try:
                cls.server.wait(timeout=8)
            except subprocess.TimeoutExpired:
                cls.server.kill()
                cls.server.wait(timeout=5)
        if hasattr(cls, "log"):
            cls.log.close()
        cls.directory.cleanup()

    def check_call(self, transport, nfs, client="allowed", ca="ca", host="localhost", mode="normal"):
        result = subprocess.run([
            OPTIONS.native_client, host + ":" + str(self.port), str(self.root / (ca + ".crt")),
            str(self.root / (client + ".crt")), str(self.root / (client + ".key")),
            mode, str(transport), str(nfs)], capture_output=True, text=True, timeout=8)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr[-2000:]
                         + (self.root / "server.log").read_text()[-4000:])

    def test_native_read_returns_range_and_eof_over_mutual_tls(self):
        self.check_call(0, 0, mode="read")
        self.check_call(0, 0, mode="read-full")
        self.check_call(0, 0, mode="read-eof")

    def test_native_read_maps_authorization_and_stale_handle(self):
        self.check_call(0, 13, mode="read-denied")
        self.check_call(0, 70, mode="read-stale")
        self.check_call(0, 10001, mode="read-badhandle")
        self.check_call(16, 5, mode="read-unauthenticated")
        self.check_call(16, 5, mode="read-embedded-nul")
        self.check_call(7, 5, mode="read-wrong-export")

    def test_native_getattr_and_lookup_return_verified_attributes(self):
        self.check_call(0, 0, mode="getattr")
        self.check_call(0, 0, mode="lookup")
        self.check_call(0, 0, mode="lookup-unicode")

    def test_native_create_returns_file_handle_and_transfer_id(self):
        self.check_call(0, 0, mode="create")

    def test_native_mkdir_returns_directory_handle(self):
        self.check_call(0, 0, mode="mkdir")

    def test_native_remove_is_forwarded_over_mutual_tls(self):
        self.check_call(0, 0, mode="remove")

    def test_native_rename_is_forwarded_over_mutual_tls(self):
        self.check_call(0, 0, mode="rename")

    def test_native_write_reports_file_sync_after_durable_staging_over_mutual_tls(self):
        self.check_call(0, 0, mode="write")
        self.check_call(0, 0, mode="write-recover")

    def test_native_commit_and_close_are_forwarded_over_mutual_tls(self):
        self.check_call(0, 0, mode="commit")
        self.check_call(0, 0, mode="close")

    def test_native_getattr_and_lookup_preserve_nfs_failures(self):
        self.check_call(0, 70, mode="getattr-stale")
        self.check_call(0, 13, mode="getattr-denied")
        self.check_call(0, 13, mode="lookup-denied")
        self.check_call(3, 5, mode="lookup-invalid-name")

    def test_native_readdir_returns_entries_with_or_without_backend_verifier(self):
        self.check_call(0, 0, mode="readdir")
        self.check_call(0, 0, mode="readdir-next")
        self.check_call(0, 0, mode="readdir-no-verifier")

    def test_native_readdir_preserves_cookie_and_authorization_errors(self):
        self.check_call(0, 10027, mode="readdir-invalid-verifier")
        self.check_call(0, 13, mode="readdir-denied")
        self.check_call(3, 5, mode="readdir-invalid-count")
        self.check_call(3, 5, mode="readdir-invalid-cookie-size")

    def test_native_read_rejects_unbounded_or_unreceivable_ranges_locally(self):
        self.check_call(3, 5, mode="read-invalid-args")
        self.check_call(3, 5, mode="read-invalid-principal")
        self.check_call(3, 5, mode="read-null-principal")
        self.check_call(3, 5, mode="read-small-buffer")

    def test_authorized_tls_peer_receives_implemented_capabilities(self):
        self.check_call(0, 0)

    def test_peer_cannot_access_another_export(self):
        self.check_call(7, 5, mode="wrong-export")

    def test_ca_trust_does_not_authorize_unlisted_peer(self):
        self.check_call(7, 5, client="unlisted")

    def test_untrusted_server_is_rejected(self):
        self.check_call(14, 5, ca="other-ca")

    def test_backend_hostname_is_verified(self):
        self.check_call(14, 5, host="127.0.0.1")

    def test_expired_client_certificate_is_rejected(self):
        self.check_call(14, 5, client="expired")

    def test_unknown_protocol_version_is_rejected(self):
        self.check_call(9, 5, mode="unsupported-version")

    def test_ganesha_loads_ozone_export_and_completes_capability_handshake(self):
        if not OPTIONS.ganesha_image:
            self.skipTest("Ganesha runtime image was not supplied")
        self.capabilities_marker.unlink(missing_ok=True)
        (self.root / "recovery").mkdir()
        endpoint_host = "host.docker.internal" if platform.system() == "Darwin" else "localhost"
        docker_network = "bridge" if platform.system() == "Darwin" else "host"
        proxy = None
        endpoint_port = self.port
        if platform.system() == "Darwin":
            proxy = _TcpProxy(("0.0.0.0", 0), ("127.0.0.1", self.port))
            proxy.start()
            endpoint_port = proxy.port
        (self.root / "ganesha.conf").write_text('''
NFS_CORE_PARAM {
    Protocols = 4;
    Enable_UDP = false;
    Enable_RQUOTA = false;
    Clustered = true;
    NFS_Port = 2049;
}

NFS_KRB5 {
    PrincipalName = "%s";
    KeytabPath = "%s";
    Active_krb5 = %s;
}

NFSv4 {
    Minor_Versions = 1;
    Server_Scope = "ozone-nfs";
    Server_Owner = "ozone-nfs";
    Graceless = false;
    Lease_Lifetime = 60;
    Grace_Period = 90;
    Delegations = false;
    RecoveryBackend = fs_ng;
    RecoveryRoot = "/contract/recovery";
    RecoveryDir = "ozone-v4recov";
}

EXPORT {
    Export_Id = 1;
    Path = "/";
    Pseudo = "/ozone/data";
    Access_Type = RW;
    Protocols = 4;
    Transports = TCP;
    SecType = %s;
    Squash = Root_Squash;
    Attr_Expiration_Time = 0;

    FSAL {
        Name = OZONE;
        Endpoint = "%s:%d";
        Export_UUID = "00112233-4455-6677-8899-aabbccddeeff";
        Volume_ID = 1;
        Bucket_ID = 2;
        Trusted_CA_File = "/contract/ca.crt";
        Client_Certificate_File = "/contract/allowed.crt";
        Client_Key_File = "/contract/allowed.key";
        Timeout_Millis = 5000;
    }
}
''' % ("nfs/localhost@EXAMPLE.COM" if OPTIONS.ganesha_keytab else "nfs",
       "/contract/nfs.keytab" if OPTIONS.ganesha_keytab else "",
       "true" if OPTIONS.ganesha_keytab else "false", "krb5p" if OPTIONS.ganesha_keytab else "sys",
       endpoint_host, endpoint_port))
        container_name = "ozone-nfs-ganesha-contract-{}".format(os.getpid())
        log_path = self.root / "ganesha.log"
        mounted = False
        with open(log_path, "w") as ganesha_log:
            docker_command = [
                "docker", "run", "--rm", "--name", container_name,
                "--network", docker_network,
                "--mount", "type=bind,source={},target=/contract".format(self.root)]
            if OPTIONS.ganesha_keytab:
                docker_command.extend([
                    "--mount", "type=bind,source={},target=/contract/nfs.keytab,readonly".format(
                        OPTIONS.ganesha_keytab),
                    "--mount", "type=bind,source=/etc/krb5.conf,target=/etc/krb5.conf,readonly"])
            docker_command.extend([
                OPTIONS.ganesha_image, "sh", "-c",
                "rpcbind -w && exec ganesha.nfsd -I 0 -F -L STDOUT "
                "-p /contract/ganesha.pid -f /contract/ganesha.conf"])
            process = subprocess.Popen(docker_command, stdout=ganesha_log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while not self.capabilities_marker.exists():
                    if process.poll() is not None:
                        self.fail("Ganesha exited before its capability handshake:\n"
                                  + log_path.read_text()[-20000:])
                    if time.monotonic() >= deadline:
                        self.fail("Ganesha did not call the backend:\n" + log_path.read_text()[-20000:])
                    time.sleep(0.1)
                time.sleep(1)
                self.assertIsNone(process.poll(), log_path.read_text()[-20000:])
                if OPTIONS.mount_contract:
                    mount = OPTIONS.mount_contract
                    mount.mkdir(parents=True, exist_ok=True)
                    mount_result = subprocess.run([
                        "mount", "-t", "nfs4", "-o", "vers=4.1,proto=tcp,sec=krb5p,port=2049",
                        "localhost:/ozone/data", str(mount)], capture_output=True, text=True, timeout=30)
                    if mount_result.returncode != 0:
                        self.fail("NFSv4.1 krb5p mount failed:\n{}{}\nGanesha log:\n{}".format(
                            mount_result.stdout, mount_result.stderr, log_path.read_text()[-20000:]))
                    mounted = True
                    subprocess.run([
                        sys.executable,
                        str(Path(__file__).resolve().parents[1] / "nfs" / "test_ganesha_readonly.py"),
                        "--mount", str(mount)], check=True, timeout=60)
            finally:
                if mounted:
                    subprocess.run(["umount", str(OPTIONS.mount_contract)], check=False, timeout=15)
                subprocess.run(["docker", "rm", "--force", container_name],
                               check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                subprocess.run([
                    "docker", "run", "--rm",
                    "--mount", "type=bind,source={},target=/contract".format(self.root),
                    OPTIONS.ganesha_image, "chown", "-R",
                    "{}:{}".format(os.getuid(), os.getgid()), "/contract/recovery"],
                    check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if proxy is not None:
                    proxy.close()
        self.assertTrue(self.capabilities_marker.exists(), log_path.read_text()[-20000:])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--java-classpath", required=True)
    parser.add_argument("--native-client", required=True)
    parser.add_argument("--ganesha-image")
    parser.add_argument("--ganesha-keytab")
    parser.add_argument("--mount-contract", type=Path)
    OPTIONS, remaining = parser.parse_known_args()
    if bool(OPTIONS.ganesha_keytab) != bool(OPTIONS.mount_contract):
        parser.error("--ganesha-keytab and --mount-contract must be supplied together")
    if OPTIONS.mount_contract and platform.system() != "Linux":
        parser.error("--mount-contract requires Linux")
    unittest.main(argv=["test_grpc_tls.py"] + remaining)
