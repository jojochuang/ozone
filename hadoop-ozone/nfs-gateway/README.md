<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# NFS gateway development

This directory contains the Java mutual-TLS listener and peer/export authorization, a versioned backend proto, an optional native C++ gRPC adapter, native file-handle codecs, and transport/mounted-file contracts. The FSAL_OZONE module implements GETATTR, LOOKUP, READDIR, bounded READ, CREATE, MKDIR, REMOVE, RENAME, and NFSv4 write callbacks against patched Ganesha V5.7. At export startup, the FSAL retries only gRPC `UNAVAILABLE` and `DEADLINE_EXCEEDED` capability requests for up to 60 seconds; TLS, authentication, protocol, and capability errors still fail export creation. The gateway can publish durable staged ranges into the canonical Ozone file under an OM lease fence, then clean up ranges on final close. Its capability response names CREATE, WRITE, and COMMIT only when the export lease manager is configured. Linux/aarch64 NFSv4.1 `sec=krb5p` mounted contracts pass read operations, file create/write/fsync/COMMIT/rename/close/remove, and directory create/remove against the Java fixture over mutual TLS. The secure-cluster publication contract verifies OzoneFS readback after mounted file writes and directory create/remove, and confirms that truncation and append opens on existing files are rejected without changing their data; NFS writes apply to new file transfers. Same-host process/container recovery and persistent stale-handle behavior have mounted coverage; separate-host recovery over shared durable storage remains pending. The gateway advertises these capabilities so the FSAL can reject incomplete backends at export startup.

Writable exports on an existing OM cluster require the NFS layout features through version 15 to be finalized and the object-index bootstrap to complete. Check OM finalization and index readiness before diagnosing an OM `NOT_SUPPORTED_OPERATION` on WRITE as a gateway defect; do not bypass the layout or readiness gates.

OM exposes the NFS object-index bootstrap phase as the `NfsObjectIndexStatus` attribute on `Hadoop:service=OzoneManager,name=OzoneManagerInfo,component=ServerRuntime`. It reports `NOT_FINALIZED`, `PENDING`, `DIRECTORIES`, `FILES`, `READY`, `UNAVAILABLE`, or `UNKNOWN` without exposing the database cursor.

The `nfs-native` GitHub Actions workflow builds the Java gateway and pinned Ganesha FSAL, runs gateway unit tests, secure MiniOzoneCluster and FSO identity/readiness integration tests, including caller authorization and keytab-based Kerberos ticket renewal, native handle and C++/Java mutual-TLS tests, Python mounted-contract runner tests, and a live FSAL-to-Java capability handshake. It also builds the Ozone distribution and runs `test_distribution.py` to verify the packaged gateway launcher and classpath. Run [39520838982](https://github.com/jojochuang/ozone/actions/runs/39520838982) passed on commit `7d6bad159dd`. The hosted workflow does not mount a Linux kernel NFS client against Ozone; full mounted read/write acceptance against a healthy secure Ozone cluster remains open.

The standalone gateway exports per-operation Metrics2 counters for backend RPC count, NFS status errors, gRPC failures, and average latency in nanoseconds. Metric names follow `Num<Operation>Ops`, `Num<Operation>NfsStatusErrors`, `Num<Operation>GrpcFailures`, and `<Operation>LatencyNsAvgTime`; the operation is one of Capabilities, Getattr, Lookup, Readdir, Read, Create, Write, Commit, Mkdir, Remove, or Rename. The source is registered for the lifetime of the Java gateway process. These are backend gRPC metrics, not NFS client RPC counts or end-to-end performance measurements.

On 2026-10-15, Java WRITE replies with NFS `FILE_SYNC` after the replicated staging object and OM range manifest are durable. The secure HA integration test verifies that reply and confirms a replacement gateway can recover and publish the staged data. Its two secure tests passed under Linux/JDK 21; the native gRPC/TLS contract passed 21 tests with one optional Ganesha-runtime test skipped. The gateway service unit suite passed 31 tests. The secure integration fixture also configures a two-range per-file cap, stages two out-of-order ranges, and verifies the third returns NFS `DELAY` without changing data; both tests pass with this cap enabled.

On 2026-10-16, the Linux/JDK 21 Maven `test` reactor passed across all 42 modules for `TestNfsCallerAuthorizationSecure#startsSecureGatewayAndAcquiresExportLeaseOverMutualTls` (1 test, 0 failures, 0 errors). The test crashes a gateway after a durable staged write and before intent promotion, then verifies a replacement gateway recovers the staged bytes and removes the fenced pending intent and object before COMMIT; OzoneFS readback is byte-exact. It also verifies that an expired acknowledged range is deleted after replacement-gateway recovery when retention is set to zero. The same-day mounted Linux file-transfer contract passed all 7 cases. The OM lease unit suite also passes with configured recovery retention boundary and persistence coverage (9 tests, 0 failures). The NFS staging-store suite passes 6/6, including idempotent deletion when the object is already absent; NFS gateway Checkstyle reports 0 violations. Protobuf backward-compatibility checks pass for the changed Ozone client, HDDS server, and HDDS admin interfaces when run with Linux-native `protolock`. These were local Docker runs, not hosted CI; cleanup is triggered on a subsequent file-lease recovery, and separate-host shared-storage recovery remains open.

On 2026-10-16, the secure HA crash-recovery integration test also left one intent after the staging object closed and another after OM PREPARE but before object creation. Replacement-gateway recovery removed both intents, tolerated the absent object, and still published the acknowledged bytes exactly; the test passed 1/1 in the Linux/JDK 21 reactor. The OM range suite passed 9/9 with a regression for a late promotion after lease recovery has marked its intent `CLEANUP_PENDING`. Gateway unit suites passed 30/30, including resuming an already cleanup-pending range after its object was deleted, and integration-test Checkstyle reports 0 violations. This exercises the crash states locally; separate-host shared-`RecoveryRoot` acceptance remains pending.

On 2026-10-16, the secure Linux mounted 32 MiB contract reproduced an immediate zero-byte read after write/fsync against the pre-change gateway. Later reads returned the expected bytes and SHA-256. The mount used `actimeo=0`. OM audit's `dataSize=0` is a request argument and does not show the response size; follower-read proxy log entries also do not identify that specific lookup. NFS object, child, range-manifest, and file-lease lookups now request leader-only linearizable consistency. The common translator unit suite passes 18/18, including assertions on all four NFS read hints. After packaging that change and restarting only the NFS gateway daemon, its first mounted read-after-write run failed while the new daemon was recovering the prior export lease. After the gateway settled, three consecutive immediate 32 MiB reads and the repository's exact publication contract passed with byte-exact data. In a controlled stop/start rerun, the replacement daemon waited for the prior export lease to expire, then reported listening; once ready, the complete mounted publication suite passed 11 tests with 3 optional checks skipped, including another byte-exact 32 MiB upload. This is one successful restart-window rerun; repeat the scenario before treating the intermittent read as resolved. A local macOS secure-gateway integration attempt stopped during MiniOzoneCluster startup when SCM RPCs to localhost timed out.

On 2026-10-18, the secure two-export mounted NFSv4.1 `sec=krb5p` acceptance passed 13 enabled tests with only the optional TGT-expiry test skipped. The actimeo=0 export passed byte-exact 32 MiB publication, configured-key readback, concurrent transfers, export isolation, stale-handle `ESTALE`, and rename/open-handle checks. A separate mounted renewal contract shortened the disposable gateway principal's maximum ticket life to two minutes and passed 1/1 after waiting 130 seconds beyond the original TGT; the same gateway completed a second NFS write/fsync and byte-exact OzoneFS readback. The original one-day principal lifetime was restored and the gateway restarted. A further complete mounted rerun passed 13 enabled tests with the same optional skip. The earlier zero-byte read failure has not recurred in these runs, but its cause remains unconfirmed. Separate-physical-host `RecoveryRoot` recovery, authenticated live metrics, representative performance, and packaging/PMC-Legal review remain open.

The mounted OzoneFS publication contract also runs GNU `cp` uploads from local storage to the NFS mount and compares the mounted and OzoneFS bytes. It asserts that `chmod` is rejected with `EOPNOTSUPP` or `EPERM` while the file data stays unchanged; `EPERM` is expected when root squashing prevents the caller from owning the file. A live Linux mount experiment on 2026-10-15 showed `cp -p` returning an error when timestamp preservation is unsupported, while the copied bytes remained readable. The mounted contract now checks that this failure is reported and that the file bytes remain available through NFS and OzoneFS. All 13 publication contract tests passed against the secure Linux NFSv4.1 `sec=krb5p` mount; four optional checks were skipped because that run did not configure Ozone key readback, a second export, a short-lived gateway TGT, or an `actimeo=0` mount. The earlier run also verified byte-exact readback of EC and KMS-encrypted keys.

## Stable export configuration

The Java binding loader accepts named properties in the gateway's persisted Ozone configuration. For an export named `data`, all six properties are required:

```properties
ozone.nfs.export.data.uuid=00112233-4455-6677-8899-aabbccddeeff
ozone.nfs.export.data.cluster.id=<expected Ozone cluster ID>
ozone.nfs.export.data.volume=<volume name>
ozone.nfs.export.data.bucket=<bucket name>
ozone.nfs.export.data.volume.id=<native volume ID in unsigned decimal>
ozone.nfs.export.data.bucket.id=<native bucket ID in unsigned decimal>
```

Choose the UUID once when creating an export and retain it with the exact cluster and bucket incarnation across gateway replacement. The example UUID is illustrative. Remapping an export requires a new UUID; startup never generates one automatically. IDs use unsigned decimal notation even when their Java `long` representation is negative. Export configuration names contain only letters, digits, underscores, and hyphens.

`NfsExportBinding.fromConfiguration` rejects missing properties, invalid UUIDs, invalid unsigned IDs, and ambiguous configuration names. Handle validation requires the actual connected cluster ID from trusted backend discovery and rejects another export or incarnation. OM still validates live metadata and checks caller ACLs. Loading these properties does not start a gateway or grant access; trusted cluster discovery and an export-ready capability handshake remain prerequisites.

## Gateway process

The distribution includes the Java backend and exposes it as `ozone nfs`. After building a distribution, run the package smoke test to verify the gateway jar, classpath, and launcher. The launcher reaches the secure-mode check when invoked without secure configuration. It requires Ozone secure mode, a service Kerberos principal and keytab, a readable server certificate/private key/CA bundle, and at least one Ganesha client certificate SHA-256 fingerprint. Configure the listener to a loopback address; it rejects non-loopback binds. After setting the gateway and export properties above, start it with:

```sh
python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_distribution.py \
  hadoop-ozone/dist/target/ozone-*-SNAPSHOT
```

Set the listener identity and TLS trust explicitly in `ozone-site.xml`. Fingerprints are 64 hexadecimal SHA-256 characters without colons; the server certificate must be valid for the hostname configured in the Ganesha FSAL.

```properties
ozone.security.enabled=true
ozone.nfs.gateway.address=127.0.0.1
ozone.nfs.gateway.port=9878
ozone.nfs.gateway.kerberos.principal=nfs-gateway/_HOST@EXAMPLE.COM
ozone.nfs.gateway.kerberos.keytab.file=/etc/security/keytabs/nfs-gateway.keytab
ozone.nfs.gateway.server.certificate.file=/etc/ozone/nfs-gateway.crt
ozone.nfs.gateway.server.private.key.file=/etc/ozone/nfs-gateway.key
ozone.nfs.gateway.trusted.ca.file=/etc/ozone/ganesha-client-ca.crt
ozone.nfs.gateway.ganesha.cert.sha256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

The gateway service principal needs proxy-user rules for NFS callers and permissions for export lease operations. Create the private staging bucket and configure its volume and bucket separately before enabling writes. Mounted writes, Ozone publication, same-gateway restart/reclaim, and changed-hostname replacement-container reclaim using persisted Ganesha recovery state pass against a real Ozone export. On 2026-10-15, two Linux mounted runs kept a file descriptor open across Ganesha container restart, then read, appended, fsynced, verified final bytes, and cleaned up using the persisted bind-mounted `RecoveryRoot`. A same-day regression rerun passed the mounted write contract (3/3), OzoneFS publication contract (9 passed, 3 skipped), and separate `actimeo=0` stale-handle validation (1/1). Fifteen consecutive 16 MiB uploads then passed byte-exact readback through NFS and OzoneFS. The publication contract now includes a 32 MiB upload; it passed with byte-exact NFS and OzoneFS readback, followed by eight consecutive 32 MiB mounted write/fsync/readback cycles. These Docker-host tests do not verify separate physical hosts or remote shared-storage recovery, which remain production gates.

The gateway uses OM's returned absolute `expiresAtMillis` value for both export and file leases, converts the remaining time to a monotonic local deadline, and fails closed if OM returns an already-expired lease. Each NFS file lease lasts half the configured export-lease duration, while export and file leases renew every third of that duration. This leaves time for the export renewal to complete before a file lease renewal would cross the current export-lease expiry. Mounted open-file tests verified append, fsync, and byte-exact readback after stopping the Java gateway beyond the 30-second export lease and restarting it. The full mounted contract has also had intermittent zero-filled readbacks. OM audit evidence from one failure shows a successful 16 MiB COMMIT_KEY followed about 390 ms later by a second 0-byte COMMIT_KEY for the same file. Ganesha invokes FSAL `close2` for read-only opens too, and the FSAL previously finalized every close without checking the state share mode; a read-only close could therefore recover a fresh empty lease and publish it over the file. `close2` now skips finalization unless the state includes write access. The pinned native image builds successfully. Its first fresh-mount run completed write/fsync, but rename returned `EIO` because OM still saw an active NFS writer lease; directory create/remove passed. Instrumentation showed `close2` received share access 2 for the writer and 1 for a reader, and the writer's final COMMIT returned NFS OK. OM audit then showed RELEASE followed 93 ms later by RECOVER and RENEW for the same object. The Java READ path previously recovered a lease whenever staged reads returned no data; the current path checks OM's published marker before recovering. Two consecutive full secure mounted runs after the certificate renewal passed write, rename, concurrent transfer, export isolation, and byte-exact 16 MiB publication. A Ganesha restart with an open descriptor and a separate `actimeo=0` stale-handle test also passed. Keep the earlier zero-filled readback report as historical evidence and repeat acceptance on hosted Linux before treating that issue as fully closed.

On 2026-10-15, a mounted run initially failed after Docker restarted because the generated test TLS certificate had expired. A direct mTLS probe identified the expired certificate; renewing the local test certificate and updating the OM test configuration restored Ganesha capability negotiation and both NFS exports. The basic mounted write contract passed 3/3. Three consecutive secure OzoneFS contract runs each passed 9 tests, including 16 MiB byte-exact publication, four concurrent transfers, copy upload, stable rename, directory operations, export isolation, chmod rejection, and truncate rejection; each run skipped optional configured-key readback and short-lived gateway TGT checks. A separate `actimeo=0` stale-handle test and a Ganesha restart/reclaim test with an open file descriptor both passed; the stale-handle test passed again after the latest Docker start. The earlier Linux error 524 and zero-byte COMMIT failure did not recur, though their cause is unconfirmed. The Java lease-manager fix that renews the export lease before each background file-lease renewal remains covered by 19 passing unit tests.

On 2026-10-16, the pinned Ganesha V5.7 image rebuilt successfully after `ozone_reopen2` was corrected to retain refreshed attributes. The secure mounted OzoneFS contract passed all 12 enabled checks against the primary export, including 32 MiB byte-exact NFS and OzoneFS readback, directory create/remove, rename, stale handles, concurrent transfers, and export isolation; configured-key readback and gateway TGT-expiry checks were skipped because they were not configured. Before this full run, a focused directory-removal check on the long-lived primary test bucket first failed with `NFS_LEASE_CONFLICT` while old file leases were reclaimed after a gateway restart. After unmounting and remounting the client, those leases stopped renewing, OM audited the focused delete successfully, and the full primary-export contract passed. One intermediate full run hit the Ozone CLI's 60-second timeout streaming the 32 MiB object; the focused retry completed byte-exactly in 10.5 seconds and the subsequent full run passed. A pre-fix run also had one empty 32 MiB NFS readback; five consecutive 32 MiB publication cycles passed after the FSAL refresh change. Hosted Linux acceptance remains necessary.

The gateway also requires a stable, unique `ozone.nfs.gateway.id`. It acquires an OM-owned lease for every configured export before listening, renews each lease every third of its configured duration, and stops serving an export immediately after a renewal failure. On clean shutdown it releases its leases. The default duration is 30 seconds; `ozone.nfs.gateway.export.lease.duration` accepts durations from 3 seconds through 60 seconds. The gateway service principal must have write access to each exported bucket for lease operations.

Durable range staging uses a separate operator-created native FSO bucket. Configure both names, keep the bucket outside every exported namespace, and grant the gateway service principal read/write access. Do not expose this bucket to NFS, OzoneFS, or S3 users. Staged objects and published files use their bucket's default replication, falling back to the gateway's configured RATIS factor and then Ozone's RATIS/THREE default. NFS writes fail closed for EC replication. The gateway accepts at most 256 outstanding ranges per file by default; configure `ozone.nfs.gateway.max.outstanding.ranges.per.file` to change the limit. Once full, it returns NFS `DELAY` for new ranges while allowing exact retries. At OM layout version 15, the gateway records a `PENDING_STAGE` intent with the deterministic key before creating the immutable object, then promotes it to `RECORDED` after close commits the object with the configured replication. Java WRITE dispatch completes this sequence before returning NFS `FILE_SYNC`; COMMIT publishes recorded ranges into the canonical file, and the gateway recovers a lease from the persistent file handle after restart. OM range lookup and commit size validation ignore pending intents. The lease owner reads recorded ranges until OM records that the canonical file commit succeeded; other callers read the currently published canonical file subject to normal Ozone ACLs. After the published marker is set, reads use the canonical file even if cleanup left range manifests behind. COMMIT assembles staged data into the existing canonical file under the current OM fence; final CLOSE removes range manifests and staged objects before releasing the lease. On takeover, the replacement gateway marks old-generation, unacknowledged stage intents for cleanup, deletes their deterministic objects idempotently, and removes the OM rows. Acknowledged ranges remain recoverable until `ozone.om.nfs.recovery.retention` elapses after the persisted file-lease expiration (default `7d`). OM evaluates that deadline using the recovery request's replicated leader timestamp and moves expired acknowledged manifests to `CLEANUP_PENDING` in the same transaction as file-lease recovery; the gateway then deletes the deterministic objects idempotently and removes their OM rows. Cleanup of inactive files is lazy and starts when a subsequent file-lease recovery is requested. A secured HA integration test kills the gateway JVM after staging a file, then verifies a replacement gateway recovers and publishes byte-exact data. Mounted writes and publication pass against the secure cluster. On 2026-10-14, a Linux NFSv4.1 client also reclaimed an open file across replacement Ganesha containers with different hostnames, the same NFS address and explicit server identity, distinct gateway IDs, and a shared local bind-mounted `RecoveryRoot`; the replacement gateway acquired its OM lease after expiry, and read/write/fsync completed with byte-exact final contents. Separate physical hosts and remote shared-storage recovery remain unverified.

```properties
ozone.nfs.gateway.id=nfs-gateway-01
ozone.nfs.gateway.export.lease.duration=30s
ozone.nfs.gateway.staging.volume=nfs-system
ozone.nfs.gateway.staging.bucket=write-staging
ozone.nfs.gateway.max.outstanding.ranges.per.file=256
```

When replacing a gateway, use a distinct ID for the replacement process. OM permits a new gateway to take over after the previous export lease expires.

The pinned Ganesha server configuration example is [ganesha.conf.example](src/main/ganesha/ganesha.conf.example). It enables only NFSv4.1, requires Kerberos privacy, and keeps Ganesha's recovery database in `RecoveryRoot`. Create that directory before starting Ganesha and make it writable by the Ganesha service account. For fenced active/passive replacement, both hosts must use the same durable `RecoveryRoot`, `Clustered = true`, the same explicit Ganesha node ID (`ganesha.nfsd -I 0`), export identity, NFS address, `Server_Scope`, and `Server_Owner`. Keep exactly one instance active; `fs_ng` does not provide active/active recovery. The Java gateway is loopback-only and owns the OM export lease, so it must also start on the replacement host and acquire the lease before that host serves requests. On 2026-10-14, the secure mounted contract passed across containers with different hostnames: it reclaimed an open file and verified read, append, fsync, and byte-exact contents after replacement. The replacement gateway waited for the prior OM export lease to expire before it started serving. This used a shared bind-mounted `RecoveryRoot` on one Docker host; separate physical hosts and remote shared-storage behavior remain production gates. The pinned V5.7 source maps the shared recovery path to `RecoveryRoot/RecoveryDir/node0` for this configuration. Automatic failover remains unsupported.

```sh
ozone nfs
```

Use `ozone --daemon start nfs` for background operation. The Ganesha FSAL connects to the loopback backend over mutual TLS. The certificate allowlist currently grants each configured Ganesha certificate access to all configured exports; do not use one certificate where separate per-export access is required.

## Native handle contract

From the repository root, with CMake and a C99 compiler installed:

```sh
cmake -S hadoop-ozone/nfs-gateway -B /tmp/ozone-nfs-native -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/ozone-nfs-native
ctest --test-dir /tmp/ozone-nfs-native --output-on-failure
```

Java and C share golden payloads in `hadoop-ozone/common/src/test/resources/nfs/file-handles.txt`. The native test also checks every header byte value and truncated payload length.

## Mounted-file contract

Build the patched upstream Ganesha V5.7 runtime and FSAL_OZONE for Linux from the pinned commit:

```sh
docker build -f hadoop-ozone/nfs-gateway/src/main/ganesha/Dockerfile \
  -t ozone-nfs-ganesha:local hadoop-ozone
docker run --rm ozone-nfs-ganesha:local ganesha.nfsd -v
```

This multi-stage image builds pinned Ganesha V5.7 and FSAL_OZONE, then keeps only the daemon, FSAL, and runtime libraries. It copies the example configuration to `/etc/ganesha/ganesha.conf.example`; mount an operator-owned configuration at `/etc/ganesha/ganesha.conf` and the referenced TLS files under `/etc/ozone`. Persist `/var/lib/nfs/ganesha` across process restarts. On Linux, use host networking when the configured Java gateway endpoint is `127.0.0.1:9878`; this image is an external runtime prototype and is not bundled into the Ozone distribution.

Ganesha is LGPL-3.0-or-later at the pinned source revision. This Dockerfile is for local experimentation only: it does not yet package the Ganesha license and corresponding source/relink materials required for redistribution. Do not publish or redistribute an image built from it until Apache licensing review approves the boundary and its notices/source packaging. See the [licensing and distribution review in the NFS design](../../hadoop-hdds/docs/content/design/nfs-ganesha.md#ganesha-licensing-and-distribution-boundary).

For a Linux host with the Java gateway already running on loopback:

```sh
docker run --rm --network host \
  --mount type=bind,src=/etc/ganesha/ganesha.conf,dst=/etc/ganesha/ganesha.conf,readonly \
  --mount type=bind,src=/etc/ozone,dst=/etc/ozone,readonly \
  --mount type=bind,src=/var/lib/ganesha/ozone,dst=/var/lib/nfs/ganesha \
  ozone-nfs-ganesha:local ganesha.nfsd -I 0 -F -L STDOUT -f /etc/ganesha/ganesha.conf
```

On Linux, use Python 3 against an existing NFSv4.1 mount:

```sh
timeout -k 5 300 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_file_transfer.py --mount /mnt/ozone
timeout -k 5 60 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_readonly.py --mount /mnt/ozone
timeout -k 5 60 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_write.py --mount /mnt/ozone
timeout -k 5 180 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_reclaim.py \
  --mount /mnt/ozone --restart-command 'sudo systemctl restart nfs-ganesha'
# For a separate-host acceptance run, the command fences the active host, starts
# the standby on the shared RecoveryRoot, and moves the NFS address. The identity
# command must report the host currently serving that same NFS endpoint.
# The RecoveryRoot identity command must return a stable storage identity (for
# example, the shared filesystem source and UUID) from the active host.
# Configure the same Ganesha node ID, server scope, owner, and export identity on
# the standby; start its gateway with a new ID and wait for the OM lease takeover.
# The server identity command reports those stable NFSv4 identity fields and the
# NFS address, while the serving-host command must report a different host.
timeout -k 5 300 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_reclaim.py \
  --mount /mnt/ozone \
  --restart-command 'ssh nfs-control /usr/local/sbin/promote-ganesha-standby' \
  --serving-host-id-command 'ssh nfs-control /usr/local/sbin/active-ganesha-machine-id' \
  --recovery-root-identity-command 'ssh nfs-control /usr/local/sbin/active-ganesha-recovery-root-id' \
  --server-identity-command 'ssh nfs-control /usr/local/sbin/active-ganesha-server-identity' \
  --restart-timeout-seconds 240
# Run only in a disposable Linux client; kinit replaces its default ticket cache.
kinit -kt /etc/security/keytabs/testuser.keytab -l 1m testuser/om@EXAMPLE.COM
mount -t nfs4 -o vers=4.1,sec=krb5p nfs-gateway.example.com:/ozone /mnt/ozone
timeout -k 5 180 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_credential_expiry.py \
  --mount /mnt/ozone --ticket-cache FILE:/tmp/krb5cc_0
timeout -k 5 120 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_ozone.py \
  --mount /mnt/ozone --ozone-uri ofs://om-host/volume/bucket \
  --read-key ec-file --read-key encrypted-file
# Repeatedly restart the Java gateway, wait for its export-ready probe, then
# verify a byte-exact 32 MiB NFS upload and canonical OzoneFS readback.
timeout -k 5 1800 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_restart_acceptance.py \
  --mount /mnt/ozone --ozone-uri ofs://om-host/volume/bucket \
  --restart-command 'ssh om-host "ozone --daemon stop nfs && ozone --daemon start nfs"' \
  --instance-id-command 'ssh om-host "pgrep -f org.apache.hadoop.ozone.nfs.gateway.NfsGatewayDaemon"' \
  --ready-command 'ssh om-host "nc -z 127.0.0.1 9878"' --cycles 10
# In a disposable test KDC, shorten the gateway principal's maximum ticket life,
# restart the Java gateway, then verify a mounted write after that lifetime.
timeout -k 5 600 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_ozone.py \
  --mount /mnt/ozone --ozone-uri ofs://om-host/volume/bucket \
  --gateway-tgt-lifetime-seconds 120
# Optional: provide a second NFSv4.1 sec=krb5p mount and its distinct FSO bucket.
timeout -k 5 180 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_ozone.py \
  --mount /mnt/ozone --ozone-uri ofs://om-host/volume/bucket \
  --isolated-mount /mnt/ozone-isolated \
  --isolated-ozone-uri ofs://om-host/volume/other-bucket
timeout -k 5 900 python3 hadoop-ozone/nfs-gateway/src/test/nfs/benchmark_ganesha_ozone.py \
  --mount /mnt/ozone
# Optional: include OM request-counter deltas from a reachable, non-interactive JMX endpoint.
timeout -k 5 900 python3 hadoop-ozone/nfs-gateway/src/test/nfs/benchmark_ganesha_ozone.py \
  --mount /mnt/ozone --om-jmx-url http://om-host:9874/jmx
# In secure mode, configure hdds.prometheus.endpoint.token on OM and provide the
# same secret to the benchmark as a file; /prom accepts its bearer token.
timeout -k 5 900 python3 hadoop-ozone/nfs-gateway/src/test/nfs/benchmark_ganesha_ozone.py \
  --mount /mnt/ozone --om-prometheus-url http://om-host:9874/prom \
  --om-prometheus-token-file /run/secrets/om-prometheus-token
```

For server-side resident-memory measurements, repeat `--process name=pid` for the local Ganesha, NFS gateway, and OM PIDs. The benchmark samples `/proc/<pid>/status` every 250 ms during both workloads and includes each process's peak RSS and sample count in its JSON output. Run it where those PIDs are visible; a Ganesha container needs host PID visibility or the container's PID namespace.

The runners reject local paths and other NFS versions using `/proc/mounts`. The file-transfer runner creates a temporary directory inside the mount and removes only its test contents. Run it on an export where the test identity may create, read, rename, and remove files and directories. The Ozone publication runner also uploads four distinct 512 KiB files concurrently, fsyncs each, and compares every result with its canonical OzoneFS object. The read-only Ganesha runner requires an NFSv4.1 `sec=krb5p` mount and the Java contract fixture's `child` file; it verifies listing, a ranged read, EOF, and that opening the file for writing is denied. The writable Ganesha runner uses the Java mock fixture's RW export and checks create, staged file size, write, fsync/COMMIT, exact byte readback, rename with stable inode identity, removal, and directory create/remove over the mount. It exercises mock publication only, not Ozone data persistence or crash durability. The reclaim runner requires a command that restarts or replaces Ganesha, keeps a file open across the change, then verifies read, write, and fsync through the reclaimed open state. Persist the configured `RecoveryRoot` and keep the export identity and NFS address unchanged; a replacement must use the saved recovery data before accepting clients. Run only against a test export because the command restarts the server. The credential-expiry runner requires a disposable client with a short-lived TGT and NFS service ticket; it verifies an operation works before expiry and is rejected after the service ticket expires. The Ozone publication runner targets a real FSO bucket mounted through Ganesha and checks out-of-order and overlapping writes, fsync, byte-exact OzoneFS readback, rename with stable inode identity, and removal through both interfaces. Its stale-handle case requires a client mount with `actimeo=0`; it permanently deletes and recreates a test key through Ozone, verifies the new path has a new file ID, and expects the old open handle to return `ESTALE`. Pass both `--isolated-mount` and `--isolated-ozone-uri` to compare same-name, different-content files through a second export and confirm that each mount and OzoneFS bucket sees only its own key. Both mounts must be NFSv4.1 `sec=krb5p`, and the Ozone CLI must be configured to access both buckets. The outer timeout bounds a stalled hard NFS mount.

Coverage includes fresh-file upload and fsync, ranged reads and EOF, empty files, directory listing, file and parent-directory rename with stable inode identity, removal, nonempty-directory errors, and UTF-8 names. Successful fsync here checks the syscall result; host-loss durability requires separate crash tests. Listing all entries does not prove mutation-safe directory cookies.

The benchmark emits JSON with sequential write-plus-fsync throughput, read throughput and checksum, small-file create/write/fsync/delete throughput and latency percentiles, NFS per-operation RPC counter deltas from `/proc/self/mountstats`, mount options, and client process peak RSS. Optional `--process` arguments add sampled peak RSS for Ganesha, the gateway, and OM. Optional `--om-jmx-url` adds active OM `Num*` counter deltas when the endpoint is reachable without interactive authentication. For secure clusters, configure `hdds.prometheus.endpoint.token` on OM and pass its secret file with `--om-prometheus-url` and `--om-prometheus-token-file`; the runner sends the bearer token and reads the same counters from `/prom`. A secure mounted test returned 403 without the token and 200 with it, and the benchmark completed its authenticated scrape; that run did not parse any OM counter deltas. Do not put the token directly in command-line arguments. Unrelated traffic on the client mount or OM is included in the counter deltas, so use an isolated test stack for interpretation. The `NumKeyOps`-per-NFS-RPC ratio is diagnostic, not a stable performance promise. The seven file-transfer tests pass against upstream Ganesha V5.7 MEM on Linux/aarch64 with AUTH_SYS. Mounted read-only and writable contracts pass against the Java fixture over FSAL_OZONE with RPCSEC_GSS/Kerberos privacy and mutual TLS. The writable contract verifies staged size before COMMIT, exact mock byte readback afterward, rename with stable inode identity, removal, and directory operations. The Ozone publication runner passes against the secure test cluster and verifies OzoneFS byte-exact readback after mounted writes. On 2026-10-14, the six-test secure mounted contract passed against two NFSv4.1 `sec=krb5p` exports: same-name files with different contents remained isolated in both mounts and their distinct FSO buckets, with different file IDs and byte-exact OzoneFS readback. The FSO mini-cluster contracts verify that Ozone key overwrite/rename, OzoneFS ancestor rename/delete, and S3 Gateway overwrite/delete/multipart part upload are blocked while an NFS file lease is active; rejected overwrites preserve the published bytes, and S3 returns `ConditionalRequestConflict` (409). On 2026-10-14, the standalone reclaim runner passed against a secure Linux NFSv4.1 `sec=krb5p` mount across replacement Ganesha containers with different hostnames and gateway IDs: it kept a file open, reclaimed state using a shared `RecoveryRoot` on a host bind mount, then verified read/write/fsync plus byte-exact final contents. This validates container replacement on one Docker host; separate-host shared-storage recovery and persistent stale handles remain separate acceptance requirements in the OEP.

On 2026-10-15, the credential-expiry runner passed on a disposable Linux client with a one-minute TGT. It created and removed a directory to obtain an NFS service ticket, waited past ticket expiry, then confirmed a new `mkdir` was rejected.

The Ozone publication runner's optional `--gateway-tgt-lifetime-seconds` case requires a disposable test KDC whose gateway principal has the specified maximum TGT lifetime; restart the gateway after changing it. The contract writes and verifies a file before expiry, waits ten seconds beyond the configured lifetime, then verifies a second mounted write/COMMIT and canonical OzoneFS readback. Restore the principal lifetime after testing, and never change a production KDC for this test. On 2026-10-15, the secure Docker stack passed with a two-minute gateway TGT: after the original ticket expired, the same gateway JVM completed a mounted 180,224-byte write and fsync with byte-exact OzoneFS readback. A focused follow-up restarted the Java gateway for 35 seconds with an open descriptor. Read, append, fsync, byte-exact NFS and OzoneFS readback, close, and unlink all succeeded. An earlier cleanup stall did not recur in two follow-up runs; keep this path under repeat acceptance.

## C++/Java mutual TLS contract

The user approved the optional native gRPC C++/protobuf dependencies on 2026-10-08. The Java module builds with Maven and reuses Ozone's existing gRPC/protobuf dependencies:

```sh
mvn -pl :ozone-nfs-gateway -am install -DskipTests -DskipShade -DskipRecon -DskipDocs
mvn -pl :ozone-nfs-gateway test dependency:copy-dependencies -DincludeScope=test -DskipShade -DskipRecon -DskipDocs
```

Build the Linux transport toolchain and run the contract using the repository mounted at `/workspace`:

```sh
docker build -t ozone-nfs-grpc:local -f hadoop-ozone/nfs-gateway/src/test/native/Dockerfile.grpc hadoop-ozone/nfs-gateway
docker run --rm -v "$PWD:/workspace:ro" ozone-nfs-grpc:local sh -c '
  cmake -S /workspace/hadoop-ozone/nfs-gateway -B /tmp/nfs-build -DOZONE_NFS_GRPC=ON -DCMAKE_BUILD_TYPE=Debug &&
  cmake --build /tmp/nfs-build -j2 &&
  ctest --test-dir /tmp/nfs-build --output-on-failure &&
  python3 /workspace/hadoop-ozone/nfs-gateway/src/test/native/test_grpc_tls.py     --java-classpath "/workspace/hadoop-ozone/nfs-gateway/target/classes:/workspace/hadoop-ozone/nfs-gateway/target/test-classes:/workspace/hadoop-ozone/interface-client/target/classes:/workspace/hadoop-ozone/common/target/classes:/workspace/hadoop-ozone/nfs-gateway/target/dependency/*"     --native-client /tmp/nfs-build/test_ozone_grpc_tls -v'
```

Use your configured Docker context. The image records native package versions in `/opt/ozone-nfs-native-versions.txt`. The verified Linux/aarch64 prototype uses gRPC C++ 1.51.1-4.1build5 and protobuf/compiler 3.21.12-8.2ubuntu0.3; the Java side uses the repository's gRPC 1.77.1 and protobuf 3.25.9. Native generated sources use the same installed compiler/runtime. This is prototype interoperation evidence, not a supported production dependency baseline.

The 21 live tests cover CREATE, READ, WRITE, COMMIT/final close, GETATTR, LOOKUP, and READDIR interoperation over mutual TLS, including UTF-8 lookup, directory continuation and stale verifier handling, export isolation, a CA-trusted but unlisted peer, expired client certificates, backend hostname verification, untrusted server trust, Ganesha export startup, and unsupported service versions. The C adapter also refuses missing TLS material or an unbounded/zero deadline. Six Java tests cover peer/export policy and reject plaintext, non-loopback peers, expired or unlisted certificates, and empty/ambiguous allowlists.

The fixture creates temporary test credentials and exercises GETATTR, LOOKUP, READDIR, and READ plus the backend CREATE, WRITE, COMMIT, and close paths. Capability negotiation includes CREATE when the export lease manager is present. These tests prove wire/TLS behavior, not a KDC login or Ozone data access. Production caller operations still require the authenticated RPCSEC_GSS principal and OM authorization described in the OEP. The mounted secure-cluster contract separately verifies real Ozone reads and write publication through Ganesha and OzoneFS.

## Caller authorization

`NfsCallerAuthorizer` first requires the pinned TLS peer to be authorized for the requested export. It accepts a realm-bearing Kerberos principal with an integrity or privacy protection assertion, applies the configured Hadoop Kerberos name rules, and authorizes the resulting proxy user through an initialized Hadoop `ImpersonationProvider`. The real user must have Kerberos service credentials. Proxy host authorization uses the backend transport peer address; the local Ganesha peer must authenticate the original NFS caller through RPCSEC_GSS before making this assertion.

`NfsGatewayDaemon` installs its Hadoop UGI configuration before login and enables Hadoop's keytab-based Kerberos credential renewal for the long-running service process. It initializes the operator's impersonation rules and constructs this authorizer before starting the backend. Each operation uses the returned proxy UGI for its Ozone client; a shared service-user client would lose the caller identity. Secure gateway startup, KDC login, and OM export-lease acquisition are covered by the HA mini-cluster integration test. `TestNfsGatewayKerberosRenewal` also uses MiniKDC with a 30-second maximum TGT lifetime and verifies that the daemon's keytab login renews its TGT before expiry. The optional mounted renewal acceptance now confirms that an Ozone write and fsync still succeed after the gateway TGT expires and is renewed.

Two caller tests cover both protection levels, mapped user and real-user identity, denied users and hosts, missing protection, invalid or unmapped principals, absent TLS context, and service users without Kerberos credentials. Service credentials are mocked: these tests do not prove KDC login or Ganesha's RPCSEC_GSS extraction.

`src/test/nfs/test_ganesha_authorization.py` is a mounted contract for Ozone ACL enforcement. Run it as a non-owner Linux user with a valid Kerberos ticket after allowing that user in both the gateway and OM proxy-user rules:

```sh
python3 src/test/nfs/test_ganesha_authorization.py --mount /mnt/ozone
```

On 2026-10-13, the secure Linux NFSv4.1 `sec=krb5p` mount returned `EACCES` to `testuser2` when OM denied access to the private export root. The normal `testuser` mounted read/write contract still passes with the default proxy-user rule.

`NfsObjectReader` connects an authorized caller to the existing `LookupNfsObject` OM RPC. It rejects mismatched request exports, malformed handles, and cluster/export/incarnation mismatches before opening an OM transport. Transport creation and lookup run under the returned UGI, with no pathname fallback or shared service-user connection; each transport closes on success or failure. Head lookup omits block locations, while data lookup requests the latest authorized locations. `NfsGatewayDaemon` obtains the cluster ID from SCM, validates export bindings, and wires the reader into the production gRPC service. The secure-cluster mounted contracts verify Ozone reads and writes through Ganesha, and a second Kerberos user reaches the OM ACL check and receives `EACCES` when creating in the private export root. That denial test exposed an OM volume/bucket lock-order error; NFS object and child lookups now resolve bucket links and check caller ACLs outside the bucket lock, then revalidate the object path before returning metadata or listing children. Same-gateway NFSv4 restart/reclaim passed on 2026-10-13; recovery from a separate physical host remains unverified.

Four lookup unit tests verify caller identity during transport creation and submission, unchanged identity after the operation, resource cleanup after OM failure, authorization failure before connection, and invalid binding/handle rejection. The FSO identity integration contract exercises this reader through real OM RPC for metadata, block locations, rename continuity, and stale identity after recreation. It also sends capability, GETATTR, LOOKUP, READDIR, and ranged-read calls over the production mutual-TLS gateway server against real mini-cluster data. Its caller authorizer is mocked in a non-secure mini-cluster, so it does not prove Kerberos caller authorization or a mounted Ganesha path to real Ozone data.

## Identity-based ranged reads

`RpcClient.readNfsFile` opens a file by volume/bucket/object identity through OM and uses the same identity for block pipeline refresh. It reuses Ozone's existing input-stream construction, including EC and encryption handling; it never refreshes by the mutable pathname. A client unit test exercises refresh after rename, deletion, and a mismatched replacement response. The secure HA MiniOzoneCluster contract verifies byte-exact EC-backed and KMS-encrypted reads through the production Java gateway over mutual TLS. `test_ganesha_ozone.py --read-key KEY` compares named files through the mounted FSAL against OzoneFS; mounted EC and KMS-encrypted key readback passed on 2026-10-13, while repeating that check in the current Docker acceptance run remains pending.

`NfsObjectReader.read` performs the read under the authorized caller's UGI, closes the client and stream, and limits each request to 1 MiB. Offset is an unsigned 64-bit value: offsets beyond EOF return empty data with EOF, including values above Java's signed range. Zero count before EOF returns empty data without EOF. Premature storage EOF fails rather than reporting successful end of file. Directory reads consult OM before returning the directory error, preserving stale identity and ACL failures.

`NfsGatewayService` exposes GETATTR, LOOKUP, READDIR, READ, MKDIR, REMOVE, and RENAME. With an export lease manager configured, it also exposes CREATE, WRITE, and COMMIT for durable new-file transfers. It enforces TLS/export context before dispatch. OM errors become explicit NFS status responses, malformed handles become BADHANDLE, and storage failures become IO. Hadoop RPC authorization failures map to NFS ACCESS, and the native FSAL translates NFS IO to its protocol status. Failure responses omit data and EOF. Authentication errors retain their gRPC status, while unexpected backend failures expose only INTERNAL. Mounted writes, Ozone publication, and replacement-container reclaim using persisted Ganesha recovery state are verified; separate-host shared-storage recovery remains a production gate. Root handles are bound to the configured bucket identity and each operation still validates live identity and caller ACLs through OM.

The 84 gateway unit tests cover dispatch, error categories, read bounds, resource cleanup, identity authorization, durable write ranges, and lease recovery. The FSO mini-cluster contract also exercises GETATTR, LOOKUP, READDIR, and real datanode reads through the gateway reader and mutual-TLS gRPC server: partial and final ranges, zero-count and unsigned beyond-EOF reads, empty files, renamed files and parents, stale reads after recreation, and mutation-safe directory continuation. The FSO mini-cluster contract does not establish Ganesha callbacks against real Ozone data, mounted NFS, Kerberos end to end, or crash durability; those have separate secure integration and mounted contracts.

## Native C++ READ interoperation

The C ABI provides `ozone_grpc_create`, `ozone_grpc_write`, `ozone_grpc_commit`, `ozone_grpc_read`, `ozone_grpc_getattr`, `ozone_grpc_lookup`, and `ozone_grpc_readdir`, with explicit export UUID, caller identity/protection, persistent handles, and bounded outputs. CREATE is a negotiated backend capability when the export lease manager is configured; the FSAL also implements WRITE callbacks. READ and WRITE are limited to 1 MiB and READDIR to 256 entries; calls use a gRPC deadline, clear outputs on failure, and reject malformed responses. Caller principal bytes use an explicit length and are never truncated at NUL.

Twenty-one live Linux C++/Java mTLS tests exercise CREATE, READ data and EOF, WRITE, COMMIT and final close, GETATTR/LOOKUP attributes and errors, READDIR continuation and `NOT_SAME` verifier handling, export isolation, denied callers, certificate policy, Ganesha export startup, and unsupported versions. The fixture invokes the production `NfsGatewayService` dispatcher with a mock reader; it proves wire/TLS behavior, including range data, not a KDC login or Ozone data access. The mini-cluster contract separately covers real Ozone/datanode reads. Ganesha loads the module, initializes an export, and completes its TLS capability handshake against the fixture in a Linux startup smoke test. Mounted contracts verify fixture operations, while secure-cluster publication and replacement-container reclaim verify real Ozone data through Ganesha; separate-host shared-storage recovery remains open.

The live native/Java test service checks caller principal and privacy fields in its test reader. An embedded-NUL principal survives the C ABI and wire, then receives UNAUTHENTICATED; oversized and null principal buffers fail locally. These are identity-transport checks with a mock reader, not a Kerberos/GSS login test.
