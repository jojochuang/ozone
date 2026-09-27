# S3 compatibility CI harness

On-demand GitHub Actions workflow [`.github/workflows/s3-compatibility.yml`](../../.github/workflows/s3-compatibility.yml) runs [ceph/s3-tests](https://github.com/ceph/s3-tests) and [minio/mint](https://github.com/minio/mint) against Ozone using the orchestration from [peterxcli/ozone-s3-compatibility](https://github.com/peterxcli/ozone-s3-compatibility).

## Triggers

- Comment `/s3-compat` on a pull request.
- Manual **workflow_dispatch** (optional `ref`, `pr_number`, suite toggles).

Results are compared to the latest **mainstream nightly** baseline published at [ozone.s3.peterxcli.dev](https://ozone.s3.peterxcli.dev/) (Parquet under `gh-pages/data/catalog` and `gh-pages/data/runs/`). The job fails when the overall verdict is **regression**.

## Harness pin and overrides

The workflow checks out the external harness at `S3_COMPAT_HARNESS_REF` (see the workflow env) and applies [`apply-harness-overrides.sh`](apply-harness-overrides.sh), which copies:

- Local Ozone tree support in `clone_sources.sh` (`OZONE_REPO` as a directory).
- Verdict output in `compare_runs.py` (`regression` / `no change` / `improved`).

When these changes land upstream, bump `S3_COMPAT_HARNESS_REF` and remove the overrides.

## Local checks

```bash
python3 dev-support/s3-compatibility/tests/test_compare_verdict.py -v
# Requires a clone of ozone-s3-compatibility at /tmp/ozone-s3-compatibility:
bash dev-support/s3-compatibility/tests/test_clone_sources_local.sh
```
