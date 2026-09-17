#!/usr/bin/env python3
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

"""Merge compression benchmark JSON files into a markdown report."""

import json
import sys
from pathlib import Path


def human_size(num_bytes: int) -> str:
    if num_bytes >= 1024 * 1024:
        return f"{num_bytes // (1024 * 1024)}MB"
    if num_bytes >= 1024:
        return f"{num_bytes // 1024}KB"
    return f"{num_bytes}B"


def load_json_files(result_dir: Path):
    rows = []
    meta = []
    for path in sorted(result_dir.glob("results-*.json")):
        with path.open() as f:
            doc = json.load(f)
        meta.append({
            "file": path.name,
            "scenario": doc.get("scenario"),
            "gitSha": doc.get("gitSha"),
            "timestamp": doc.get("timestamp"),
        })
        for row in doc.get("rows", []):
            row = dict(row)
            row["_source"] = doc.get("scenario")
            rows.append(row)
    return rows, meta


def main():
    repo_root = Path(__file__).resolve().parent.parent
    result_dir = repo_root / "hadoop-ozone" / "integration-test" / "target" / "compression-benchmark"
    out_path = repo_root / "dev-support" / "compression-benchmark" / "REPORT.md"

    if len(sys.argv) > 1:
        result_dir = Path(sys.argv[1])
    if len(sys.argv) > 2:
        out_path = Path(sys.argv[2])

    if not result_dir.is_dir():
        print(f"No results in {result_dir}", file=sys.stderr)
        sys.exit(1)

    rows, meta = load_json_files(result_dir)
    if not rows:
        print("No benchmark rows found", file=sys.stderr)
        sys.exit(1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Transparent compression benchmark report",
        "",
        "Generated from MiniOzone integration benchmarks (`-Dozone.run.benchmark=true`).",
        "",
        "## Run metadata",
        "",
        "| Result file | Scenario | Git SHA | Timestamp |",
        "|-------------|----------|---------|-----------|",
    ]
    for m in meta:
        lines.append(
            f"| {m['file']} | {m['scenario']} | {m['gitSha']} | {m['timestamp']} |"
        )

    lines.extend([
        "",
        "## Latency and throughput",
        "",
        "| Scenario | Codec | Size | Op | Samples | Mean ms | Median ms | "
        "Mean MB/s | Median MB/s | Stored/Logical |",
        "|----------|-------|------|----|---------|---------|-----------|"
        "-----------|-------------|----------------|",
    ])

    def sort_key(r):
        return (r.get("_source", ""), r.get("codec", ""), r.get("fileSizeBytes", 0),
                r.get("operation", ""))

    for row in sorted(rows, key=sort_key):
        ratio = row.get("storedToLogicalRatio")
        ratio_s = f"{ratio:.3f}" if ratio is not None else "—"
        size = human_size(int(row["fileSizeBytes"]))
        lines.append(
            f"| {row.get('_source', '')} | {row['codec']} | {size} | {row['operation']} | "
            f"{row['samples']} | {row['meanMillis']:.2f} | {row['medianMillis']:.2f} | "
            f"{row['meanThroughputMbPerSec']:.2f} | {row['medianThroughputMbPerSec']:.2f} | "
            f"{ratio_s} |"
        )

    lines.extend([
        "",
        "## Notes",
        "",
        "- **BASELINE** / `pre-compression-baseline`: commit before transparent compression.",
        "- **NONE**: compression feature enabled but bucket codec is NONE.",
        "- Payload is deterministic compressible bytes (pattern `i % 251`).",
        "- Default workload: 5 files per round, 1 warmup round, 3 measured rounds.",
        "",
    ])

    out_path.write_text("\n".join(lines))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
