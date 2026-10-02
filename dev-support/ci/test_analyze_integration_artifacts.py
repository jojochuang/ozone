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
# See the License for the License for the specific language governing permissions and
# limitations under the License.

import tempfile
import unittest
from pathlib import Path

from analyze_integration_artifacts import parse_log, parse_total_time_to_seconds
from analyze_surefire_method_timing import collect_testcases


class TestAnalyzeIntegrationArtifacts(unittest.TestCase):
    def test_parse_total_time_to_seconds(self) -> None:
        self.assertEqual(parse_total_time_to_seconds("01:10 h"), 4200.0)
        self.assertEqual(parse_total_time_to_seconds("02:05 min"), 125.0)
        self.assertEqual(parse_total_time_to_seconds("45 s"), 45.0)

    def test_parse_log_timing_gaps(self) -> None:
        log = """
[INFO] Scanning for projects...
[INFO] Running org.apache.hadoop.ozone.om.TestFoo
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 10.5 s -- in org.apache.hadoop.ozone.om.TestFoo
[INFO] Running org.apache.hadoop.ozone.om.TestBar
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 5.0 s -- in org.apache.hadoop.ozone.om.TestBar
[INFO] Total time:  02:00 min
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "output.log"
            path.write_text(log, encoding="utf-8")
            stats = parse_log(path)
            self.assertEqual(stats["total_time"], "02:00 min")
            self.assertEqual(stats["sum_class_elapsed_min"], round(15.5 / 60, 1))
            self.assertGreater(stats["pre_test_log_fraction"], 0)
            self.assertGreater(stats["estimated_non_test_min"], 0)

    def test_collect_testcases_from_xml(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.apache.hadoop.ozone.om.TestFoo" tests="2" failures="0" errors="0" skipped="0">
  <testcase name="fast" classname="org.apache.hadoop.ozone.om.TestFoo" time="0.1"/>
  <testcase name="slow" classname="org.apache.hadoop.ozone.om.TestFoo" time="12.5"/>
</testsuite>
"""
        with tempfile.TemporaryDirectory() as tmp:
            reports = Path(tmp) / "surefire-reports"
            reports.mkdir()
            (reports / "TEST-org.apache.hadoop.ozone.om.TestFoo.xml").write_text(xml, encoding="utf-8")
            cases = collect_testcases(Path(tmp))
            self.assertEqual(cases[0][1], "org.apache.hadoop.ozone.om.TestFoo")
            self.assertEqual(cases[0][2], "slow")
            self.assertEqual(cases[0][0], 12.5)


if __name__ == "__main__":
    unittest.main()
