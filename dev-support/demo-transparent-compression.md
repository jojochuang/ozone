# Transparent compression demo (Docker Compose)

Step-by-step demo for **Phase 1 transparent at-rest compression**: bucket-level ZSTD,
client-side compress on write, compressed bytes on datanodes, transparent decompress on read.

Automated script: [`demo-transparent-compression.sh`](demo-transparent-compression.sh)

Design: [`hadoop-hdds/docs/content/design/transparent-compression.md`](../hadoop-hdds/docs/content/design/transparent-compression.md)

## Prerequisites

- Docker Engine and Compose v2 (`docker compose version`)
- JDK 17 for building from source
- Enough RAM for SCM, OM, Recon, S3 Gateway, and three datanodes

## 1. Build the distribution

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
cd /path/to/ozone

mvn -pl hadoop-ozone/dist -am package -Pdist -DskipTests -Dmaven.test.skip=true \
  -DskipShade -DskipRecon -DskipDocs -Dmaven.javadoc.skip=true -Djacoco.skip=true
```

```bash
export OZONE_DIST=/path/to/ozone/hadoop-ozone/dist/target/ozone-2.3.0-SNAPSHOT
```

Adjust the version directory if your `pom.xml` version differs.

## 2. Start the cluster

Compose uses `apache/ozone-runner` and bind-mounts the dist tree at `/opt/hadoop`.

```bash
docker pull apache/ozone-runner:20260626-1-jdk25

cd "$OZONE_DIST/compose/ozone"
OZONE_REPLICATION_FACTOR=3 OZONE_DATANODES=3 ./run.sh -d
docker compose ps
```

Wait until SCM, OM, and all datanodes are up (often one to two minutes on first start).

## 3. Automated demo (recommended)

From the host, run the script inside the SCM container:

```bash
cd "$OZONE_DIST/compose/ozone"
docker compose exec -T scm bash -s < /path/to/ozone/dev-support/demo-transparent-compression.sh
```

Optional overrides:

```bash
OZONE_DEMO_VOL=myvol OZONE_DEMO_CODEC=ZSTD \
  docker compose exec -T scm bash -s < /path/to/ozone/dev-support/demo-transparent-compression.sh
```

Expected result at the end:

```text
PASS: compression verified (physical stored bytes < logical bytes on datanode)
Demo complete.
```

Typical numbers for the default 256 KiB compressible payload:

| Bucket | OM `dataSize` | DN stored | DN logical | Ratio |
|--------|---------------|-----------|------------|-------|
| Plain  | 262144        | 262144    | 262144     | 1.0   |
| ZSTD   | 262144        | ~285      | 262144     | ≪ 1   |

## 4. Manual walkthrough

```bash
docker compose exec scm bash
```

Inside the container:

```bash
VOL=demo-compress-manual
PAYLOAD=/tmp/compressible.bin

python3 - <<'PY'
n = 262144
with open("/tmp/compressible.bin", "wb") as f:
    f.write(bytes((i % 251) & 0xFF for i in range(n)))
PY

ozone sh volume create "/${VOL}"
ozone sh bucket create "/${VOL}/plain"
ozone sh bucket create "/${VOL}/compressed" --compression-codec ZSTD

ozone sh bucket info "/${VOL}/compressed"

ozone sh key put "/${VOL}/plain/compressible.bin" "$PAYLOAD"
ozone sh key put "/${VOL}/compressed/compressible.bin" "$PAYLOAD"

ozone sh key info "/${VOL}/compressed/compressible.bin"

rm -f /tmp/roundtrip.bin
ozone sh key get "/${VOL}/compressed/compressible.bin" /tmp/roundtrip.bin
cmp -s "$PAYLOAD" /tmp/roundtrip.bin && echo "Round-trip OK"

ozone debug replicas chunk-info "/${VOL}/plain/compressible.bin" | jq \
  '{dataSize, compressionCodec, block: .keyLocations[0][0].blockData |
    {compressionCodec, totalStoredLen, totalLogicalLen, storedToLogicalRatio}}'

ozone debug replicas chunk-info "/${VOL}/compressed/compressible.bin" | jq \
  '{dataSize, compressionCodec, block: .keyLocations[0][0].blockData |
    {compressionCodec, totalStoredLen, totalLogicalLen, storedToLogicalRatio}}'
```

### Extension denylist

Keys such as `*.parquet` are not compressed even when the bucket uses ZSTD
(`ozone.om.compression.skip.extensions`):

```bash
echo "fake" > /tmp/data.parquet
ozone sh key put "/${VOL}/compressed/data.parquet" /tmp/data.parquet
ozone debug replicas chunk-info "/${VOL}/compressed/data.parquet" | jq \
  '{keyCodec: .compressionCodec, stored: .keyLocations[0][0].blockData.totalStoredLen}'
```

Expect `keyCodec: "NONE"` and stored size equal to logical size.

## 5. Shutdown

```bash
cd "$OZONE_DIST/compose/ozone"
docker compose down
```

After code changes, rebuild with the Maven command in section 1; containers pick up the
new tree via the bind mount without rebuilding the runner image.

## Troubleshooting

| Symptom | What to check |
|---------|-----------------|
| `NoClassDefFoundError: ZstdOutputStream` on put | Rebuild dist; client classpath needs `zstd-jni` via `hdds-common`. |
| `--compression-codec` rejected | OM layout not finalized for compression (`ozone admin om finalization status`). |
| `chunk-info` empty or errors | Cluster not ready; wait for pipelines and retry. |
| Stored size not smaller | Random/incompressible payload, denylisted extension, or encryption on the key. |

## Related CLI

```bash
ozone sh bucket create <vol>/<bucket> --compression-codec ZSTD
ozone sh bucket set-compression-codec <vol>/<bucket> --compression-codec ZSTD
ozone sh bucket info <vol>/<bucket>
```
