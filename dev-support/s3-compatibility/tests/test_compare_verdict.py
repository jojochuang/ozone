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

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "harness-overrides" / "scripts"))

import compare_runs  # noqa: E402


def suite_summary(passed: int, failed: int, errored: int = 0, skipped: int = 0) -> dict:
    eligible = passed + failed + errored
    return {
        "passed": passed,
        "failed": failed,
        "errored": errored,
        "skipped": skipped,
        "eligible": eligible,
        "compatibility_rate": round(passed / eligible, 4) if eligible else None,
    }


class CompareVerdictTests(unittest.TestCase):
    def test_regression_on_new_non_passing(self) -> None:
        baseline = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=2, failed=1),
                    "non_passing_cases": [
                        {"classname": "c", "name": "test_still", "status": "fail"},
                    ],
                }
            }
        }
        candidate = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=2, failed=2),
                    "cases": [
                        {"classname": "c", "name": "test_still", "status": "fail"},
                        {"classname": "c", "name": "test_new", "status": "fail"},
                    ],
                }
            }
        }
        verdicts = compare_runs.compute_verdicts(candidate, baseline)
        self.assertEqual("regression", verdicts["overall"])
        self.assertEqual("regression", verdicts["suites"]["s3_tests"])

    def test_improved_when_fixed_without_new_failures(self) -> None:
        baseline = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=1, failed=1),
                    "non_passing_cases": [
                        {"classname": "c", "name": "test_old", "status": "fail"},
                    ],
                }
            }
        }
        candidate = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=2, failed=0),
                    "cases": [
                        {"classname": "c", "name": "test_old", "status": "pass"},
                    ],
                }
            }
        }
        verdicts = compare_runs.compute_verdicts(candidate, baseline)
        self.assertEqual("improved", verdicts["overall"])

    def test_no_change_when_rates_and_deltas_match(self) -> None:
        baseline = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=2, failed=1),
                    "non_passing_cases": [
                        {"classname": "c", "name": "test_still", "status": "fail"},
                    ],
                }
            }
        }
        candidate = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=2, failed=1),
                    "cases": [
                        {"classname": "c", "name": "test_still", "status": "fail"},
                    ],
                }
            }
        }
        verdicts = compare_runs.compute_verdicts(candidate, baseline)
        self.assertEqual("no change", verdicts["overall"])

    def test_overall_regression_if_any_suite_regresses(self) -> None:
        baseline = {
            "suites": {
                "s3_tests": {"summary": suite_summary(passed=2, failed=0), "non_passing_cases": []},
                "mint": {
                    "summary": suite_summary(passed=1, failed=0),
                    "cases": [{"classname": "m", "name": "t", "status": "pass"}],
                },
            }
        }
        candidate = {
            "suites": {
                "s3_tests": {
                    "summary": suite_summary(passed=1, failed=1),
                    "cases": [{"classname": "c", "name": "test_new", "status": "fail"}],
                },
                "mint": {
                    "summary": suite_summary(passed=1, failed=0),
                    "cases": [{"classname": "m", "name": "t", "status": "pass"}],
                },
            }
        }
        verdicts = compare_runs.compute_verdicts(candidate, baseline)
        self.assertEqual("regression", verdicts["overall"])


class ParquetBaselineTests(unittest.TestCase):
    def test_load_latest_baseline_from_pages_data_when_available(self) -> None:
        pages_data = Path("/tmp/s3-compat-pages/data")
        harness_scripts = Path("/tmp/ozone-s3-compatibility/scripts")
        if not (pages_data / "catalog" / "runs.parquet").is_file():
            self.skipTest("gh-pages sample not cloned at /tmp/s3-compat-pages")
        if not harness_scripts.is_dir():
            self.skipTest("harness clone not found at /tmp/ozone-s3-compatibility")
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        sys.path.insert(0, str(harness_scripts))
        baseline = compare_runs.latest_run_from_pages_data(pages_data)
        self.assertIsNotNone(baseline)
        self.assertIn("suites", baseline)


if __name__ == "__main__":
    unittest.main()
