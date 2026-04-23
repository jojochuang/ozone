#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the License); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Inspect recent failed GitHub Actions runs for a workflow, download failed
job logs, and aggregate likely flaky / failure-prone test *classes*.

Requires either:
  * GitHub CLI: `gh auth login` (used if available), or
  * GITHUB_TOKEN / GH_TOKEN in the environment (classic fine-grained: repo, read access to Actions)

Example:
  dev-support/ci/flaky_build_inspector.py --repo apache/ozone --workflow post-commit.yml --event push
  dev-support/ci/flaky_build_inspector.py --max-runs 30 --html flaky-report.html
  dev-support/ci/flaky_build_inspector.py --debug 2>debug.txt   # per-run/job test classes on stderr
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import asdict, dataclass
from html import escape
from typing import Any, Generator

# Only real failures: Surefire prints M failures with "<<< FAILURE! -- in <class>", not success lines.
# Also: per-method line "[ERROR] Fqcn.testMethod -- Time ... <<< FAILURE!"
RE_SUREFIRE_CLASS_FAILURE = re.compile(
    r"(?:\[ERROR\]|Error:)\s+Tests run:.*?Failures:\s*([1-9]\d*),.*?<<<\s*FAILURE!\s*--\s*in\s+"
    r"((?:org|com)\.apache\.[a-zA-Z0-9_.$]+)\s*$"
)
RE_SUREFIRE_METHOD_FAILURE = re.compile(
    r"(?:\[ERROR\]|Error:)\s+"
    r"((?:org|com)\.apache\.\S+?)\s+--\s+Time elapsed:.*?<<<\s*FAILURE!"
)
# Log line: timestamp + Z + single FQCN (summary output from _summary.sh)
RE_GHA_LINE_CLASS = re.compile(
    r"^\d{4}-\d{2}-\d{2}T[0-9:\.]+Z ((?:org|com)\.apache\.[a-zA-Z0-9_.$]+)(?:\r)?$"
)

RE_HAS_LOWER_METHOD = re.compile(r"^test[a-zA-Z0-9_]*$|^[a-z].*")


@dataclass
class TestHit:
  """One occurrence: test class (normalized) in a failed run."""

  test_class: str
  run_id: int
  run_url: str
  run_title: str
  job_name: str
  job_id: int


@dataclass
class TestAggregate:
  test_class: str
  count: int
  runs: int
  example_run_url: str
  run_ids: list[int]


def _to_test_class(qualified: str) -> str:
  """Map 'Classname.method' or 'Class#method' to outer test class FQCN."""
  q = qualified.strip()
  for sep, idx in (("#", 0), (".", -1)):
    if sep in q:
      if sep == "#":
        return q.split("#", 1)[0]
      head, last = q.rsplit(sep, 1)
      if last and (RE_HAS_LOWER_METHOD.match(last) is not None):
        if "$" in head:
          return head.split("$", 1)[0]
        return head
  if "$" in q and "." in q:
    return q.split("$", 1)[0]
  return q


def _summary_block_classes(lines: list[str]) -> set[str]:
  """_summary.sh cats summary.txt; those lines are timestamp + one FQCN per failure, after the step's ##[endgroup]."""
  found: set[str] = set()
  after_cmd = False
  collecting = False
  for line in lines:
    if "_summary.sh" in line and "summary.txt" in line:
      after_cmd = True
      collecting = False
      continue
    if after_cmd and "##[endgroup]" in line:
      collecting = True
      continue
    if collecting:
      s = line.strip()
      if s.startswith("##[error]") or s.startswith("##[group]"):
        break
      m2 = RE_GHA_LINE_CLASS.match(s)
      if m2 and not m2.group(1).endswith((".java", ".xml", ".yml", ".sh")):
        t = m2.group(1)
        if "JUnit Jupiter" in t or "JUnit Vintage" in t:
          continue
        found.add(_to_test_class(t))
      elif s and not s[0].isdigit() and not s.startswith("20"):
        # non-timestamp line ends the block
        if s.startswith("##["):
          break
  return found


def _extract_test_classes_from_log(text: str) -> set[str]:
  """Parse failed test *classes* from job log (Surefire ERROR failure lines and _summary.sh list)."""
  found: set[str] = set()
  lines = text.splitlines()
  for line in lines:
    m = RE_SUREFIRE_CLASS_FAILURE.search(line)
    if m:
      found.add(_to_test_class(m.group(2)))
      continue
    m = RE_SUREFIRE_METHOD_FAILURE.search(line)
    if m:
      found.add(_to_test_class(m.group(1)))
      continue
  # summary.txt (only failed TEST-*.xml) echoed by the CI failure step
  found |= _summary_block_classes(lines)
  return {c for c in found if c.count(".") >= 2}


class GitHubApi:
  def __init__(self, token: str | None) -> None:
    self._token = token
    self._user_agent = "apache-ozone-flaky-build-inspector/1.0"

  def _request(
      self, path: str, params: dict[str, str] | None = None
  ) -> Any:
    q = f"?{urllib.parse.urlencode(params)}" if params else ""
    url = f"https://api.github.com{path}{q}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": self._user_agent,
            "X-GitHub-Api-Version": "2022-11-28",
            **(
                {"Authorization": f"Bearer {self._token}"}
                if self._token
                else {}
            ),
        },
    )
    with urllib.request.urlopen(req) as resp:  # noqa: S310
      body = resp.read().decode("utf-8", errors="replace")
      if int(resp.headers.get("X-RateLimit-Remaining", 9999)) < 5:
        sys.stderr.write(
            "Warning: GitHub API rate limit almost exhausted. "
            "Set GITHUB_TOKEN for higher limits.\n"
        )
    return json.loads(body) if body else None

  def paged(
      self, path: str, params: dict[str, str] | None = None
  ) -> Generator[dict, None, None]:
    p = dict(params) if params else {}
    p.setdefault("per_page", "100")
    page = 1
    while True:
      p["page"] = str(page)
      data = self._request(path, p)
      if not isinstance(data, list):
        return
      for item in data:
        if isinstance(item, dict):
          yield item
      if len(data) < int(p.get("per_page", "100")):
        return
      page += 1

  def get_json(self, path: str, params: dict[str, str] | None = None) -> Any:
    return self._request(path, params)

  def get_raw(self, url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": self._user_agent,
            **(
                {"Authorization": f"Bearer {self._token}"}
                if self._token
                else {}
            ),
        },
    )
    with urllib.request.urlopen(req) as resp:  # noqa: S310
      if resp.geturl() != url and resp.status in (200, 302, 301):
        pass
      return resp.read().decode("utf-8", errors="replace")

  def get_job_log_text(self, owner: str, repo: str, job_id: int) -> str:
    if self._token is None:
      raise SystemExit("Downloading job logs requires GITHUB_TOKEN or GH_TOKEN.")
    # Prefer `gh api`: job logs 302 to blob storage; urllib often mis-authenticates that hop.
    try:
      if shutil.which("gh"):
        p = subprocess.run(  # noqa: S603
            [
                "gh",
                "api",
                "-H",
                "Accept: application/vnd.github.v3.raw",
                f"repos/{owner}/{repo}/actions/jobs/{job_id}/logs",
            ],
            capture_output=True,
            text=True,
            timeout=600,
            env={**os.environ, "GH_TOKEN": self._token, "GITHUB_TOKEN": self._token},
        )
        if p.returncode == 0 and p.stdout and len(p.stdout) > 50:
          return p.stdout
        if p.stderr and "rate limit" in p.stderr.lower():
          sys.stderr.write(f"gh api log: {p.stderr}\n")
    except (OSError, subprocess.SubprocessError) as e:
      sys.stderr.write(f"gh api log job {job_id} failed: {e!r}, trying HTTP\n")
    log_url = f"https://api.github.com/repos/{owner}/{repo}/actions/jobs/{job_id}/logs"
    return self.get_raw(log_url)


def _discover_token() -> str | None:
  t = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
  if t:
    return t
  try:
    p = subprocess.run(  # noqa: S603
        ["gh", "auth", "token"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if p.returncode == 0 and p.stdout.strip():
      return p.stdout.strip()
  except (OSError, subprocess.SubprocessError):
    pass
  return None


def _iter_workflow_runs(
    api: GitHubApi,
    owner: str,
    repo: str,
    workflow: str,
    max_runs: int,
    event: str | None,
    branch: str | None,
) -> list[dict[str, Any]]:
  """Fetch failed runs. The /runs response maps `workflow_runs` (not a top-level array)."""
  params: dict[str, str] = {"per_page": "100", "status": "completed"}
  if event:
    params["event"] = event
  if branch:
    params["branch"] = branch
  path = f"/repos/{owner}/{repo}/actions/workflows/{workflow}/runs"
  out: list[dict[str, Any]] = []
  page = 1
  while len(out) < max_runs:
    params_p = {**params, "page": str(page)}
    data = api.get_json(path, params_p)
    if not isinstance(data, dict):
      break
    wr = data.get("workflow_runs", [])
    for run in wr:
      if not isinstance(run, dict) or run.get("conclusion") != "failure":
        continue
      out.append(run)
      if len(out) >= max_runs:
        return out
    if len(wr) < 100:
      break
    page += 1
  return out


def _jobs_for_run(api: GitHubApi, owner: str, repo: str, run_id: int) -> list[dict]:
  path = f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
  all_jobs: list[dict] = []
  page = 1
  while True:
    data = api.get_json(path, {"per_page": "100", "page": str(page)})
    jobs: list[dict] = data.get("jobs", []) if isinstance(data, dict) else []
    all_jobs.extend(jobs)
    if len(jobs) < 100:
      break
    page += 1
  return all_jobs


def _debug_print_run_header(
    rid: int,
    title: str,
    url: str,
    fail_jobs: list[dict],
    all_job_count: int,
) -> None:
  sys.stderr.write(
      f"\n{'='*72}\n"
      f"Run {rid}\n"
      f"  title: {title}\n"
      f"  url:   {url}\n"
      f"  failed jobs (this run): {len(fail_jobs)} of {all_job_count} total jobs\n"
  )


def _debug_print_job(
    jid: int, jname: str, classes: set[str], job_index: int, job_total: int
) -> None:
  lines = sorted(classes)
  sys.stderr.write(
      f"\n  --- failed job [{job_index}/{job_total}] id={jid}\n"
      f"      name: {jname}\n"
      f"      extracted test class(es): {len(lines)}\n"
  )
  for c in lines:
    sys.stderr.write(f"        {c}\n")
  if not lines:
    sys.stderr.write("        (none — check log or parser heuristics)\n")


def _debug_print_run_union(rid: int, union: set[str]) -> None:
  lines = sorted(union)
  sys.stderr.write(
      f"\n  >>> Run {rid} — union of test classes across processed failed jobs: "
      f"{len(lines)} distinct class(es)\n"
  )
  for c in lines:
    sys.stderr.write(f"      {c}\n")
  sys.stderr.write("\n")


def collect_hits(
    api: GitHubApi,
    owner: str,
    repo: str,
    runs: list[dict[str, Any]],
    max_failed_jobs: int,
    debug: bool = False,
) -> list[TestHit]:
  hits: list[TestHit] = []
  for run in runs:
    rid = int(run["id"])
    url = str(run.get("html_url", ""))
    title = str(run.get("display_title", run.get("name", str(rid))))
    all_jobs = _jobs_for_run(api, owner, repo, rid)
    fail_jobs = [j for j in all_jobs if j.get("conclusion") == "failure"]
    run_union: set[str] = set()
    if debug:
      _debug_print_run_header(rid, title, url, fail_jobs, len(all_jobs))
    n = 0
    processed = 0
    for j in fail_jobs:
      if n >= max_failed_jobs:
        sys.stderr.write(
            f"Run {rid}: only processed first {max_failed_jobs} failed jobs (see --max-failed-jobs)\n"
        )
        break
      jid = int(j["id"])
      jname = str(j.get("name", f"job-{jid}"))
      try:
        log = api.get_job_log_text(owner, repo, jid)
      except urllib.error.HTTPError as e:
        if e.code == 403 or e.code == 404:
          sys.stderr.write(
              f"Skip job log {jid} ({e.code}): {jname!r} — {e.reason}\n"
          )
        else:
          sys.stderr.write(f"Skip job log {jid}: HTTP {e.code} {e.reason}\n")
        continue
      except OSError as e:
        sys.stderr.write(f"Skip job log {jid}: {e}\n")
        continue
      classes = _extract_test_classes_from_log(log)
      processed += 1
      run_union |= classes
      if debug:
        cap = min(len(fail_jobs), max_failed_jobs)
        _debug_print_job(jid, jname, classes, processed, cap)
      for tc in classes:
        hits.append(
            TestHit(
                test_class=tc,
                run_id=rid,
                run_url=url,
                run_title=title,
                job_name=jname,
                job_id=jid,
            )
        )
      n += 1
    if debug and fail_jobs:
      _debug_print_run_union(rid, run_union)
    elif debug and not fail_jobs:
      sys.stderr.write("  (no failed jobs in this run — nothing to parse)\n\n")
  return hits


def aggregate(hits: list[TestHit], failed_runs: int) -> list[TestAggregate]:
  by_key: dict[str, list[TestHit]] = defaultdict(list)
  for h in hits:
    by_key[h.test_class].append(h)
  rows: list[TestAggregate] = []
  for tclass, li in by_key.items():
    run_ids = {h.run_id for h in li}
    example = li[0]
    rows.append(
        TestAggregate(
            test_class=tclass,
            count=len(li),
            runs=len(run_ids),
            example_run_url=example.run_url,
            run_ids=sorted(run_ids)[:20],
        )
    )
  rows.sort(key=lambda r: (r.runs, r.count, r.test_class), reverse=True)
  return rows


def write_html(path: str, ag: list[TestAggregate], meta: dict[str, Any]) -> None:
  top = ag[: int(meta.get("top", 40))]
  max_runs = max((r.runs for r in top), default=1)
  parts = [
      "<!DOCTYPE html><html><head><meta charset='utf-8'>",
      f"<title>Flaky inspector — {escape(meta.get('repo', ''))}</title>",
      "<style>",
      "body{font:14px/1.4 system-ui,Segoe UI,sans-serif;max-width:1200px;margin:24px auto;padding:0 16px;}",
      "h1{font-size:1.25rem;}",
      "table{border-collapse:collapse;width:100%;margin-top:1rem;}",
      "th,td{border:1px solid #ccc;padding:6px 8px;text-align:left;}",
      "th{background:#f4f4f4;}",
      "tr:hover{background:#fafafa;}",
      ".bar{height:10px;background:#0a66c2;border-radius:2px;}",
      "caption{text-align:left;font-weight:600;margin-bottom:8px;}",
      "</style></head><body>",
      f"<h1>Failed test classes (heuristic)</h1>",
      f"<p>Repo: {escape(str(meta.get('repo')))} · workflow: {escape(str(meta.get('workflow')))} · ",
      f"event: {escape(str(meta.get('event', 'all')))} · failed runs analyzed: {meta.get('failed_runs', 0)}</p>",
      "<table><caption>Most often appearing in failed job logs (not necessarily flaky)</caption>",
      "<thead><tr><th>Test class</th><th>Runs</th><th>Hits</th><th>Share</th><th>Bar</th><th>Example</th></tr></thead><tbody>",
  ]
  for r in top:
    share = (r.runs / int(meta.get("failed_runs", 1) or 1)) * 100.0
    w = (r.runs / max_runs) * 100.0
    bar = f"<div class='bar' style='width:{w:.0f}%'></div>"
    ex = f"<a href='{escape(r.example_run_url)}'>{escape(r.example_run_url)}</a>"
    parts.append(
        f"<tr><td><code>{escape(r.test_class)}</code></td>"
        f"<td>{r.runs}</td><td>{r.count}</td><td>{share:.1f}%</td><td>{bar}</td><td>{ex}</td></tr>"
    )
  parts.append("</tbody></table><p><small>Hits = class seen in a failed sub-job; same class can count twice if two jobs list it. "
      "Run column counts distinct failed workflow runs. "
      "Infra/compile-only failures may yield no tests here.</small></p></body></html>")
  with open(path, "w", encoding="utf-8") as f:
    f.write("".join(parts))


def main() -> int:
  p = argparse.ArgumentParser(
      description="Aggregate test-class failures from recent failed "
      "build-branch (post-commit) GitHub Action runs."
  )
  p.add_argument("--repo", default="apache/ozone", help="owner/repo (default: apache/ozone)")
  p.add_argument(
      "--workflow",
      default="post-commit.yml",
      help="Workflow file name under .github/workflows (default: post-commit.yml)",
  )
  p.add_argument(
      "--event",
      default="push",
      help="github events filter: push, pull_request, ... (default: push)",
  )
  p.add_argument("--branch", help="e.g. master — optional branch filter")
  p.add_argument(
      "--max-runs", type=int, default=30, help="Number of failed runs to process"
  )
  p.add_argument(
      "--max-failed-jobs",
      type=int,
      default=25,
      help="Per run, cap failed job logs to avoid huge API usage",
  )
  p.add_argument("--top", type=int, default=40, help="Rows in table / HTML")
  p.add_argument("--html", help="Write HTML report to this path")
  p.add_argument("--json-out", help="Write JSON report (aggregates + hits) here")
  p.add_argument(
      "--token",
      help="GitHub token (else GITHUB_TOKEN, GH_TOKEN, or `gh auth token`)",
  )
  p.add_argument(
      "--debug",
      action="store_true",
      help="Print per-run and per-failed-job extracted test classes to stderr; summary table still on stdout",
  )
  args = p.parse_args()
  if "/" not in args.repo:
    p.error("--repo must be owner/repo")
  token = args.token or _discover_token()
  if not token:
    print(
        "A GitHub token is required to download private logs; for apache/ozone, "
        "unauthenticated public access may 403 on logs. Set GITHUB_TOKEN or run `gh auth login`.",
        file=sys.stderr,
    )
  owner, _, repo = args.repo.partition("/")
  if not owner or not repo:
    p.error("Invalid --repo")

  api = GitHubApi(token)
  runs = _iter_workflow_runs(
      api, owner, repo, args.workflow, args.max_runs, args.event, args.branch
  )
  if not runs:
    print("No failed runs found for the given filters.", file=sys.stderr)
    return 1

  if args.debug:
    sys.stderr.write(
        "=== --debug: per-run / per-failed-job parsed test classes (stderr) ===\n"
        f"    analyzing {len(runs)} failed workflow run(s). Main table below on stdout.\n"
    )
  hits = collect_hits(
      api,
      owner,
      repo,
      runs,
      max_failed_jobs=args.max_failed_jobs,
      debug=args.debug,
  )
  ag = aggregate(hits, failed_runs=len(runs))
  top_n = ag[: args.top]
  meta = {
      "repo": args.repo,
      "workflow": args.workflow,
      "event": args.event,
      "branch": args.branch,
      "failed_runs": len(runs),
      "top": args.top,
  }
  w = 0
  if args.debug:
    sys.stderr.write("=== end debug; aggregate summary table (stdout) ===\n\n")
  print(
      f"{'#':3} {'runs':5} {'hits':5}  test class\n"
      f"{'--':3} {'----':5} {'----':5}  {'-'*60}"
  )
  for i, r in enumerate(top_n, 1):
    w = i
    print(f"{i:3} {r.runs:5d} {r.count:5d}  {r.test_class}")
  if w == 0:
    print("No test class patterns extracted (compile-only or permission issue?).")
  if args.html:
    write_html(args.html, ag, meta)
    print(f"\nWrote {args.html}", file=sys.stderr)
  if args.json_out:
    out = {
        "meta": meta,
        "aggregates": [asdict(x) for x in ag],
        "hits": [asdict(h) for h in hits],
    }
    with open(args.json_out, "w", encoding="utf-8") as f:
      json.dump(out, f, indent=2)
    print(f"Wrote {args.json_out}", file=sys.stderr)
  return 0


if __name__ == "__main__":
  try:
    raise SystemExit(main())
  except KeyboardInterrupt:
    print("Interrupted", file=sys.stderr)
    raise SystemExit(130) from None
