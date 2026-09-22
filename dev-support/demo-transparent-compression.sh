#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Path B demo: CLI put/get + physical vs logical size via ozone debug chunk-info.
#
# Prerequisites:
#   - Ozone cluster with transparent compression (OM + DN layouts finalized).
#   - `ozone` and `jq` on PATH; OZONE_CONF_DIR / ozone-site.xml pointing at OM.
#
# Usage:
#   bash dev-support/demo-transparent-compression.sh
#   OZONE_DEMO_VOL=myvol OZONE_DEMO_CODEC=ZSTD bash dev-support/demo-transparent-compression.sh

set -euo pipefail

command -v ozone >/dev/null || { echo "ozone not found on PATH"; exit 1; }
command -v jq >/dev/null || { echo "jq not found on PATH"; exit 1; }

VOL="${OZONE_DEMO_VOL:-demo-compress-$(date +%s)}"
BUCKET_NONE="${OZONE_DEMO_BUCKET_NONE:-demo-none}"
BUCKET_CODEC="${OZONE_DEMO_BUCKET_CODEC:-demo-zstd}"
KEY="${OZONE_DEMO_KEY:-compressible.bin}"
CODEC="${OZONE_DEMO_CODEC:-ZSTD}"
PAYLOAD="${OZONE_DEMO_PAYLOAD:-/tmp/ozone-compressible.bin}"
PAYLOAD_BYTES="${OZONE_DEMO_PAYLOAD_BYTES:-262144}"

echo "=== Transparent compression demo ==="
echo "Volume: ${VOL}  codec bucket: ${BUCKET_CODEC} (${CODEC})  key: ${KEY}"
echo

echo "--- 1. Compressible payload (${PAYLOAD_BYTES} bytes) ---"
python3 - <<PY
n = int("${PAYLOAD_BYTES}")
with open("${PAYLOAD}", "wb") as f:
    f.write(bytes((i % 251) & 0xFF for i in range(n)))
print("Wrote", n, "bytes to ${PAYLOAD}")
PY

echo "--- 2. Namespace ---"
ozone sh volume create "/${VOL}"
ozone sh bucket create "/${VOL}/${BUCKET_NONE}"
ozone sh bucket create "/${VOL}/${BUCKET_CODEC}" --compression-codec "${CODEC}"

echo "--- 3. Bucket policy ---"
ozone sh bucket info "/${VOL}/${BUCKET_NONE}"
ozone sh bucket info "/${VOL}/${BUCKET_CODEC}"

echo "--- 4. Upload same object to both buckets ---"
ozone sh key put "/${VOL}/${BUCKET_NONE}/${KEY}" "${PAYLOAD}"
ozone sh key put "/${VOL}/${BUCKET_CODEC}/${KEY}" "${PAYLOAD}"

echo "--- 5. Logical size (OM) + round-trip read ---"
ozone sh key info "/${VOL}/${BUCKET_NONE}/${KEY}"
ozone sh key info "/${VOL}/${BUCKET_CODEC}/${KEY}"
rm -f "${PAYLOAD}.roundtrip"
ozone sh key get "/${VOL}/${BUCKET_CODEC}/${KEY}" "${PAYLOAD}.roundtrip"
cmp -s "${PAYLOAD}" "${PAYLOAD}.roundtrip"
echo "Round-trip cmp: OK (bytes match original)"

summarize_chunk_info() {
  local label="$1"
  local uri="$2"
  local json_file
  json_file="$(mktemp)"
  echo "--- 6. Physical vs logical on datanode (${label}) ---"
  echo "URI: ${uri}"
  ozone debug replicas chunk-info "${uri}" > "${json_file}"
  jq -r '
    . as $root |
    .keyLocations[0][0].blockData as $b |
    if $b == null then
      "ERROR: no blockData from chunk-info"
    else
      [
        "OM dataSize (logical): \($root.dataSize)",
        "OM key compressionCodec: \($root.compressionCodec // "NONE")",
        "DN block compressionCodec: \($b.compressionCodec)",
        "DN sum chunk len (physical stored): \($b.totalStoredLen)",
        "DN sum chunk logicalLen: \($b.totalLogicalLen)",
        "stored / logical ratio: \($b.storedToLogicalRatio)"
      ] | .[]
    end
  ' "${json_file}"
  echo
  rm -f "${json_file}"
}

summarize_chunk_info "uncompressed bucket" "/${VOL}/${BUCKET_NONE}/${KEY}"
summarize_chunk_info "compressed bucket" "/${VOL}/${BUCKET_CODEC}/${KEY}"

echo "--- 7. Assertions ---"
NONE_JSON="$(mktemp)"
CODEC_JSON="$(mktemp)"
export NONE_JSON CODEC_JSON
ozone debug replicas chunk-info "/${VOL}/${BUCKET_NONE}/${KEY}" > "${NONE_JSON}"
ozone debug replicas chunk-info "/${VOL}/${BUCKET_CODEC}/${KEY}" > "${CODEC_JSON}"

export OZONE_DEMO_EXPECT_CODEC="${CODEC}"
python3 - <<'PY'
import json
import os
import sys

expect_codec = os.environ["OZONE_DEMO_EXPECT_CODEC"]
none_path = os.environ["NONE_JSON"]
codec_path = os.environ["CODEC_JSON"]


def load(path):
    with open(path) as f:
        return json.load(f)


def block_summary(doc):
    locs = doc.get("keyLocations") or []
    if not locs or not locs[0]:
        raise SystemExit("chunk-info: no keyLocations")
    b = locs[0][0]["blockData"]
    return (
        b["totalStoredLen"],
        b["totalLogicalLen"],
        b.get("storedToLogicalRatio"),
        b.get("compressionCodec"),
        doc.get("dataSize"),
        doc.get("compressionCodec"),
    )


none = load(none_path)
codec = load(codec_path)
ns, nl, nr, nc, oms, _ = block_summary(none)
cs, cl, cr, cc, cms, ckc = block_summary(codec)

print(f"NONE bucket:  OM size={oms} DN stored={ns} logical={nl} ratio={nr}")
print(
    f"{expect_codec} bucket: OM size={cms} DN stored={cs} logical={cl} "
    f"ratio={cr} codec={cc} keyCodec={ckc}"
)

if oms != cms:
    sys.exit("OM logical dataSize differs between buckets (expected equal)")
if nl != cl or nl != oms:
    sys.exit("DN totalLogicalLen should match OM dataSize for this key")
if cs >= cl:
    sys.exit(
        f"Expected DN stored < logical for {expect_codec} bucket, "
        f"got stored={cs} logical={cl}"
    )
if cc not in (expect_codec, f"COMPRESSION_{expect_codec}"):
    sys.exit(f"Expected DN block compressionCodec {expect_codec}, got {cc}")
print("PASS: compression verified (physical stored bytes < logical bytes on datanode)")
PY

rm -f "${NONE_JSON}" "${CODEC_JSON}"
echo
echo "Demo complete."
